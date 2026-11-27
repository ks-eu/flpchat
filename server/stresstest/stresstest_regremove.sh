#!/usr/bin/env bash
set -euo pipefail

if (($# != 1)) || [[ $1 != --stress ]]; then
    printf 'Usage: %s --stress\n' "$0" >&2
    exit 2
fi

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
podman_local=(podman --remote=false)
container=
cargo_pid=
database=lightchat_stress

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

stop_cargo() {
    [[ -n $cargo_pid ]] || return 0
    kill -TERM -- "-$cargo_pid" 2>/dev/null || kill -TERM "$cargo_pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 50; attempt++)); do
        kill -0 -- "-$cargo_pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 -- "-$cargo_pid" 2>/dev/null; then
        kill -KILL -- "-$cargo_pid" 2>/dev/null || true
    fi
    wait "$cargo_pid" 2>/dev/null || true
    cargo_pid=
}

cleanup() {
    local status=$?
    trap - EXIT
    stop_cargo
    if [[ -n $container ]]; then
        if ! timeout 10s "${podman_local[@]}" rm --force --volumes "$container" >/dev/null; then
            printf 'Error: Could not remove stress container %s and its volume.\n' "$container" >&2
            ((status != 0)) || status=1
        fi
    fi
    exit "$status"
}

handle_signal() {
    local status=$1
    trap - INT TERM HUP
    stop_cargo
    exit "$status"
}

trap cleanup EXIT
trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM
trap 'handle_signal 129' HUP

for tool in cargo podman timeout setsid; do
    command -v "$tool" >/dev/null || fail "Required command not found: $tool."
done

printf 'Creating disposable PostgreSQL stress container\n'
container=$(timeout 10s "${podman_local[@]}" create \
    -p '127.0.0.1::5432' \
    -e "POSTGRES_USER=$database" \
    -e "POSTGRES_PASSWORD=$database" \
    -e "POSTGRES_DB=$database" \
    -v /var/lib/postgresql \
    docker.io/library/postgres:18.7-bookworm)
[[ -n $container ]] || fail 'Podman did not return the stress container ID.'

timeout 10s "${podman_local[@]}" start "$container" >/dev/null
published_port=$(timeout 3s "${podman_local[@]}" port "$container" 5432/tcp) \
    || fail 'Could not determine the PostgreSQL stress port.'
[[ $published_port =~ ^127\.0\.0\.1:([0-9]+)$ ]] \
    || fail "Unexpected PostgreSQL port mapping '$published_port'."
port=${BASH_REMATCH[1]}

printf 'Waiting for stress PostgreSQL readiness\n'
readiness_deadline=$((SECONDS + 90))
while ! timeout 3s "${podman_local[@]}" exec "$container" \
    pg_isready -h 127.0.0.1 -U "$database" -d "$database" >/dev/null 2>&1; do
    ((SECONDS < readiness_deadline)) || {
        timeout 5s "${podman_local[@]}" logs "$container" >&2 || true
        fail 'Timed out waiting for stress PostgreSQL readiness.'
    }
    if ! running=$(timeout 3s "${podman_local[@]}" inspect --format '{{.State.Running}}' "$container"); then
        fail 'Could not check stress container state.'
    fi
    if [[ $running != true ]]; then
        timeout 5s "${podman_local[@]}" logs "$container" >&2 || true
        fail 'Stress PostgreSQL exited before becoming ready.'
    fi
    sleep 1
done

printf 'Running registration and self-removal stress suite\n'
setsid env "LIGHTCHAT_STRESS_DATABASE_URL=postgresql://$database:$database@127.0.0.1:$port/$database" \
    cargo run --locked -p server --features regremove-stress --bin lightchat-regremove-stress -- --stress &
cargo_pid=$!
if wait "$cargo_pid"; then
    status=0
else
    status=$?
fi
cargo_pid=
exit "$status"
