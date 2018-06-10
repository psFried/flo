#!/bin/bash

USAGE=$(cat <<EOF
Starts a cluster locally for dev purposes. Will first build all targets in debug mode.
Options: 
-d <dir> Sets the root directory, under which all data directories will be created
-p <port> Starting number of a port range that instances will listen on. Will use ports sequentially from there.
-a <arg> Adds an additional argument to all flo instances started
-x Do not start any servers (will only print the flo commands that would have been run). Can be used to delete data and logs without starting servers
EOF
)
NUM_INSTANCES=3
PORT_NUM_START=3000
DATA_ROOT="${TMPDIR}"
DELETE_DATA_DIRS="no"
ADDITIONAL_ARGS=()
DRY_RUN="no"

PIDS=()

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FLO_EXE_PATH="${SCRIPT_DIR}/target/debug/flo"
IP_ADDR="127.0.0.1"

function stderr() {
    echo "$@" >&2
}

function start_instance() {
    local instance_num="$1"
    shift
    local peers="$@"
    peers=($(echo "${peers[@]//$instance_num}"))
    stderr "instance_num: ${instance_num} filtered peers: ${peers[@]}"

    local peer_ports=()
    for peer in "${peers[@]}"; do
        peer_ports+=($(($PORT_NUM_START + $peer)))
    done

    local this_port=$(($PORT_NUM_START + $instance_num))
    local this_addr="${IP_ADDR}:${this_port}"
    local peer_addr_args="${peer_ports[@]/#/-P $IP_ADDR:}"

    stderr "this_port='${this_port}', this_addr='${this_addr}', peer_args='${peer_addr_args}'"
    local command="${FLO_EXE_PATH} -d ${DATA_ROOT}/flo${instance_num} -A ${this_addr} -p ${this_port} ${peer_addr_args} ${ADDITIONAL_ARGS[@]} -o ${DATA_ROOT}/flo${instance_num}.log"
    stderr "command: $command"

    if [[ "${DRY_RUN}" == "no" ]]; then
        $command &
        local pid="$!"
        PIDS+=("${pid}")
        stderr "Started flo process with pid: ${pid}"
    fi
}

function delete_data_dirs() {
    for instance_num in $@; do
        stderr "Deleting data and log for instance: ${instance_num}"
        local dir="${DATA_ROOT}/flo${instance_num}"
        local log="${dir}.log"
        stderr "Deleting dir: '${dir}'"
        stderr "Deleting log file: '${log}'"
        if [[ -d "${dir}" ]]; then
            rm -R "${dir}"
        fi
        if [[ -f "${log}" ]]; then
            rm "${log}"
        fi
    done
}

function tail_logs() {
    local peers=$@
    local peer_logs=()
    for peer in $peers; do
        peer_logs+=( "${DATA_ROOT}/flo${peer}.log" )
    done

    tail -f "${peer_logs[@]}"
}

function stop_all() {
    for pid in ${PIDS[@]}; do
        stderr "Stopping flo pid: ${pid}"
        kill "$pid" || true
    done
}

function start_all() {
    local all_peers=($(seq 0 1 "$((NUM_INSTANCES - 1))"))
    stderr "All_peers: ${all_peers[@]}"

    # first stop any running instances
    stop_all

    if [[ "${DELETE_DATA_DIRS}" == "yes" ]]; then
        delete_data_dirs "${all_peers[@]}"
    fi

    for instance_num in "${all_peers[@]}"; do
        stderr "loop instance num: ${instance_num}"
        start_instance "$instance_num" "${all_peers[@]}"
    done

    if [[ "${DRY_RUN}" == "no" ]]; then
        tail_logs "${all_peers[@]}"
    fi
}


while getopts "n:p:d:a:rx" opt; do
    case $opt in
        n)
            NUM_INSTANCES="$OPTARG"
            stderr "Starting ${num_instances}" >&2
            ;;
        p)
            PORT_NUM_START="$OPTARG"
            stderr "Starting port number is: ${OPTARG}"
            ;;
        d)
            DATA_ROOT="$OPTARG"
            stderr "Data root is: ${OPTARG}"
            ;;
        a)
            ADDITIONAL_ARGS+=("$OPTARG")
            stderr "Using additional argument: '${OPTARG}'"
            ;;
        r)
            DELETE_DATA_DIRS="yes"
            stderr "Will delete data directories"
            ;;
        x)
            DRY_RUN="yes"
            stderr "Will be a dry run"
            ;;
        \?)
        stderr "Invalid option: -$OPTARG" >&2
        ;;
    esac
done

trap stop_all INT
start_all