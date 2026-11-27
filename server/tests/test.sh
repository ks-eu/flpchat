#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

database=lightchat_test
podman_local=(podman --remote=false)
performance_namespace=database::users::query_performance_tests::
container=
cargo_pid=
database_url=

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

mode=regular
case ${1-} in
    --performance)
        mode=performance
        shift
        ;;
    --combined)
        mode=combined
        shift
        ;;
esac

usage="Usage: $0 [--performance|--combined] [-- [--exact] FILTER [--exact]]"
filter=
exact=false
if (($#)); then
    [[ $mode != combined ]] || fail "Combined mode does not accept a test filter."
    [[ $1 == -- ]] || fail "$usage"
    shift
    if [[ ${1-} == --exact ]]; then
        exact=true
        shift
    fi
    if [[ $exact == false && $# == 2 && $2 == --exact ]]; then
        exact=true
        filter=$1
    else
        (($# == 1)) || fail "$usage"
        filter=$1
    fi
    [[ -n $filter ]] || fail "Test filter must not be empty."
fi

regular_args=(--include-ignored --skip "$performance_namespace")
performance_args=(--ignored --nocapture "$performance_namespace" --test-threads=1)
if [[ -n $filter ]]; then
    discovered=$(cargo test --locked -p server -- --list)
    selected_tests=()
    while IFS= read -r line; do
        [[ $line == *": test" || $line == *": benchmark" ]] || continue
        name=${line%: *}
        if [[ $mode == performance ]]; then
            [[ $name == "$performance_namespace"* ]] || continue
        else
            [[ $name == "$performance_namespace"* ]] && continue
        fi
        if [[ $exact == true ]]; then
            [[ $name == "$filter" ]] || continue
        else
            [[ $name == *"$filter"* ]] || continue
        fi
        selected_tests+=("$name")
    done <<< "$discovered"

    ((${#selected_tests[@]})) || fail "No $mode tests matched filter '$filter'."
    if [[ $mode == performance ]]; then
        performance_args=(--ignored --nocapture --test-threads=1 --exact "${selected_tests[@]}")
    else
        regular_args=(--include-ignored --exact "${selected_tests[@]}")
    fi
fi

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
        if ! timeout 10s "${podman_local[@]}" rm --force "$container" >/dev/null; then
            printf 'Error: Could not remove test container %s.\n' "$container" >&2
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

printf 'Creating PostgreSQL test container\n'
postgres_args=(
    -p '127.0.0.1::5432'
    -e "POSTGRES_USER=$database"
    -e "POSTGRES_PASSWORD=$database"
    -e "POSTGRES_DB=$database"
    --tmpfs /var/lib/postgresql
    docker.io/library/postgres:18.7-bookworm
)
container=$(timeout 10s "${podman_local[@]}" create "${postgres_args[@]}")
[[ -n $container ]] || fail 'Podman did not return the test container ID.'

printf 'Starting PostgreSQL test container\n'
timeout 10s "${podman_local[@]}" start "$container" >/dev/null

published_port=$(timeout 3s "${podman_local[@]}" port "$container" 5432/tcp) \
    || fail 'Could not determine the PostgreSQL container port.'
[[ $published_port =~ ^127\.0\.0\.1:([0-9]+)$ ]] \
    || fail "Unexpected PostgreSQL port mapping '$published_port'."
port=${BASH_REMATCH[1]}

printf 'Waiting for PostgreSQL readiness\n'
readiness_deadline=$((SECONDS + 60))
while ! timeout 3s "${podman_local[@]}" exec "$container" \
    pg_isready -h 127.0.0.1 -U "$database" -d "$database" >/dev/null 2>&1; do
    ((SECONDS < readiness_deadline)) || {
        timeout 5s "${podman_local[@]}" logs "$container" >&2 || true
        fail 'Timed out waiting for PostgreSQL readiness.'
    }
    if ! running=$(timeout 3s "${podman_local[@]}" inspect --format '{{.State.Running}}' "$container"); then
        fail 'Could not check PostgreSQL container state.'
    fi
    if [[ $running != true ]]; then
        timeout 5s "${podman_local[@]}" logs "$container" >&2 || true
        fail 'The PostgreSQL test container exited before becoming ready.'
    fi
    sleep 1
done
database_url="postgresql://$database:$database@127.0.0.1:$port/$database"
printf 'PostgreSQL test database ready on 127.0.0.1:%s\n' "$port"

run_cargo() {
    local status
    setsid env "DATABASE_URL=$database_url" cargo test --locked -p server -- "$@" &
    cargo_pid=$!
    if wait "$cargo_pid"; then
        status=0
    else
        status=$?
    fi
    cargo_pid=
    return "$status"
}

if [[ $mode == combined ]]; then
    regular_status=0
    performance_status=0
    printf 'Running regular tests\n'
    if run_cargo "${regular_args[@]}"; then
        :
    else
        regular_status=$?
    fi
    printf 'Running performance tests\n'
    if run_cargo "${performance_args[@]}"; then
        :
    else
        performance_status=$?
    fi
    if ((regular_status != 0)); then
        exit "$regular_status"
    fi
    exit "$performance_status"
elif [[ $mode == performance ]]; then
    printf 'Running performance tests\n'
    run_cargo "${performance_args[@]}"
else
    printf 'Running regular tests\n'
    run_cargo "${regular_args[@]}"
fi
