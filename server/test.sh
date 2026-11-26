#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

port=55432
database=lightchat_test
podman_local=(podman --remote=false)

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

case "$#:${1-}" in
    0:) test_args=(--include-ignored --skip users::query_performance_tests::) ;;
    1:--performance) test_args=(--ignored --nocapture users::query_performance_tests::) ;;
    *) fail "Usage: $0 [--performance]" ;;
esac

port_listeners() {
    ss -H -ltnp "sport = :$port"
}

container_uses_port() {
    local id=$1 mappings protocol arrow address pids pid
    shift

    mappings=$("$@" port "$id" 2>/dev/null) || mappings=
    while read -r protocol arrow address; do
        if [[ $protocol == */tcp && $arrow == '->' && $address == *":$port" ]]; then
            return 0
        fi
    done <<< "$mappings"

    # Host-network containers have no port mapping. Match their host PIDs.
    [[ $listeners == *pid=* ]] || return 1
    [[ $("$@" inspect --format '{{.HostConfig.NetworkMode}}' "$id" 2>/dev/null) == host ]] || return 1
    if [[ $1 == podman ]]; then
        pids=$("$@" top "$id" hpid 2>/dev/null) || return 1
    else
        pids=$("$@" top "$id" -eo pid 2>/dev/null) || return 1
    fi
    for pid in $pids; do
        if [[ $pid =~ ^[0-9]+$ && $listeners == *"pid=$pid,"* ]]; then
            return 0
        fi
    done
    return 1
}

stop_conflicting_containers() {
    local engine=$1 ids=$2 id
    shift 2
    for id in $ids; do
        if container_uses_port "$id" "$@"; then
            printf 'Stopping %s container %s on TCP port %s.\n' "$engine" "$id" "$port"
            "$@" stop "$id" >/dev/null || fail "Could not stop $engine container $id."
        fi
    done
}

for tool in cargo podman ss; do
    command -v "$tool" >/dev/null || fail "Required command not found: $tool."
done

listeners=$(port_listeners) || fail "Could not check TCP port $port."
podman_ids=$("${podman_local[@]}" ps -q) || fail "Could not list local Podman containers."
stop_conflicting_containers Podman "$podman_ids" "${podman_local[@]}"

if command -v docker >/dev/null; then
    for socket in "${DOCKER_HOST:-}" "unix://${XDG_RUNTIME_DIR:-/run/user/$UID}/docker.sock" unix:///var/run/docker.sock; do
        [[ $socket == unix://* && -S ${socket#unix://} ]] || continue
        docker_local=(env -u DOCKER_CONTEXT docker --host "$socket")
        if docker_ids=$("${docker_local[@]}" ps -q 2>/dev/null); then
            stop_conflicting_containers Docker "$docker_ids" "${docker_local[@]}"
        fi
    done
fi

listeners=$(port_listeners) || fail "Could not recheck TCP port $port."
if [[ -n $listeners ]]; then
    printf '%s\n' "$listeners" >&2
    fail "TCP port $port is still in use. Its listener could not be cleared through local Podman or Docker."
fi

postgres_args=(
    -p "127.0.0.1:$port:5432"
    -e "POSTGRES_USER=$database"
    -e "POSTGRES_PASSWORD=$database"
    -e "POSTGRES_DB=$database"
    --tmpfs /var/lib/postgresql
    docker.io/library/postgres:18.7-bookworm
)
container=$("${podman_local[@]}" create "${postgres_args[@]}")

cleanup() {
    local status=$?
    if ! "${podman_local[@]}" rm --force "$container" >/dev/null; then
        printf 'Error: Could not remove test container %s.\n' "$container" >&2
        if (( status == 0 )); then status=1; fi
    fi
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

"${podman_local[@]}" start "$container" >/dev/null
while ! "${podman_local[@]}" exec "$container" pg_isready -h 127.0.0.1 -U "$database" -d "$database" >/dev/null 2>&1; do
    if [[ $("${podman_local[@]}" inspect --format '{{.State.Running}}' "$container") != true ]]; then
        "${podman_local[@]}" logs "$container" >&2 || true
        fail "The test PostgreSQL container exited before becoming ready."
    fi
    sleep 1
done

DATABASE_URL="postgresql://$database:$database@127.0.0.1:$port/$database" \
    cargo test --locked -p server -- "${test_args[@]}"
