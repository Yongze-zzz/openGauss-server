#!/bin/bash
# Usage: ./build_test_imcs.sh build|init|configure|start|all
# all: build, install, initialize the data directory, and start the database.

source_dir=${SOURCE_DIR:-$(cd "$(dirname "$0")" && pwd -P)}
build_dir=${BUILD_DIR:-$source_dir/tmp_build}
install_dir=${INSTALL_DIR:-$source_dir/mppdb_temp_install}
binarylibs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc_dir=${GCC_PATH:-$binarylibs/buildtools/gcc10.3}
pgdata=${PGDATA:-/mnt/nvme1p1/gauss}
port=${PORT:-${PGPORT:-19999}}
db_name=${DB_NAME:-tpch}
jobs=${JOBS:-8}
reference_conf=${REFERENCE_CONF:-$source_dir/config/imcs_reference.conf}
dop=${DOP:-192}
numactl_args=${NUMACTL_ARGS-"--physcpubind=0-191 --membind=0-7"}
read -r -a numa_args <<< "$numactl_args"
ctl="$install_dir/bin/gs_ctl"
sql=("${GSQL_BIN:-$install_dir/bin/gsql}" -X -v ON_ERROR_STOP=1 -P pager=off -p "$port")

export GAUSSHOME="$install_dir"
export PATH="$install_dir/bin:$gcc_dir/gcc/bin:$PATH"
export LD_LIBRARY_PATH="$install_dir/lib:$gcc_dir/gcc/lib64:$binarylibs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"

stop() {
    if "$ctl" status -D "$pgdata" >/dev/null 2>&1; then
        "$ctl" stop -D "$pgdata" -m fast -w || return
    fi
}

build() {
    export THIRD_BIN_PATH="$binarylibs"
    export PREFIX_HOME="$install_dir"
    export DEBUG_TYPE=${DEBUG_TYPE:-release}
    export CC=${CC:-$gcc_dir/gcc/bin/gcc}
    export CXX=${CXX:-$gcc_dir/gcc/bin/g++}
    export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk}
    export PATH="$JAVA_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="$gcc_dir/isl/lib:$gcc_dir/mpc/lib:$gcc_dir/mpfr/lib:$gcc_dir/gmp/lib:$binarylibs/kernel/platform/python3.7/lib:$LD_LIBRARY_PATH"
    export CPLUS_INCLUDE_PATH="/usr/include/libxml2:$JAVA_HOME/include:$JAVA_HOME/include/linux:$binarylibs/kernel/platform/python3.7/include/python3.7m:$binarylibs/kernel/dependency/llvm/comm/include:${CPLUS_INCLUDE_PATH:-}"
    export C_INCLUDE_PATH="/usr/include/libxml2:$JAVA_HOME/include:$JAVA_HOME/include/linux:${C_INCLUDE_PATH:-}"

    # Follow the repository's single-node CMake build with HTAP and NUMA enabled.
    "${CMAKE_BIN:-cmake}" -S "$source_dir" -B "$build_dir" \
        -DENABLE_MULTIPLE_NODES=OFF -DENABLE_THREAD_SAFETY=ON \
        -DENABLE_READLINE=ON -DENABLE_MOT=ON -DENABLE_OPENEULER_MAJOR=ON \
        -DENABLE_HTAP=ON -DENABLE_NUMA=ON || return
    "${CMAKE_BIN:-cmake}" --build "$build_dir" --parallel "$jobs" || return

    # Stop this instance before replacing its installed binaries and libraries.
    if [[ -x $ctl ]]; then
        stop || return
    fi
    "${CMAKE_BIN:-cmake}" --install "$build_dir" || return
    echo "Installed in $install_dir"
}

init() {
    if [[ -f $pgdata/PG_VERSION ]]; then
        echo "Already initialized: $pgdata"
        return
    fi
    # Let gs_initdb prompt for the administrator password.
    "$install_dir/bin/gs_initdb" -D "$pgdata" --nodename=imcs_wise \
        -E UTF8 --locale=C -W
}

configure() {
    local node_name backup stream_attr
    [[ $dop =~ ^[1-9][0-9]*$ && $dop -gt 1 ]] ||
        { echo 'DOP must be an integer greater than 1.' >&2; return 1; }
    [[ -r $reference_conf && -f $pgdata/postgresql.conf ]] ||
        { echo 'REFERENCE_CONF and PGDATA/postgresql.conf must exist.' >&2; return 1; }

    # Preserve the identity of the initialized target instance.
    node_name=$(awk '/^[[:space:]]*pgxc_node_name[[:space:]]*=/ {
        value = substr($0, index($0, "=") + 1)
        sub(/[[:space:]]*#.*/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
    } END { print value }' "$pgdata/postgresql.conf")
    [[ -n $node_name ]] || { echo 'Target pgxc_node_name is missing.' >&2; return 1; }
    stream_attr=${THREAD_POOL_STREAM_ATTR:-"$((dop * 5)),0.2"}
    backup=$(mktemp "$pgdata/postgresql.conf.before_imcs.XXXXXX") || return
    cp -p "$pgdata/postgresql.conf" "$backup" || return
    echo "Configuration backup: $backup"
    if ! cp "$reference_conf" "$pgdata/postgresql.conf"; then
        cp -p "$backup" "$pgdata/postgresql.conf"
        return 1
    fi
    if ! "$install_dir/bin/gs_guc" set -D "$pgdata" \
        -c "port=$port" -c "pgxc_node_name=$node_name" \
        -c "thread_pool_stream_attr='$stream_attr'"; then
        cp -p "$backup" "$pgdata/postgresql.conf"
        return 1
    fi
    echo "Reference: $reference_conf; DOP: $dop; Stream: $stream_attr"
}

start() {
    local exists
    local launcher=()
    if [[ ${#numa_args[@]} -gt 0 ]]; then
        command -v numactl >/dev/null || return
        numactl "${numa_args[@]}" true || return
        launcher=(numactl "${numa_args[@]}")
    fi
    configure || return
    stop || return

    # Apply the CPU/memory policy to gaussdb through its gs_ctl parent.
    # Use this installation, without reusing an old postmaster.opts command.
    "${launcher[@]}" "$ctl" start -D "$pgdata" -Z single_node -p "$install_dir/bin/gaussdb" \
        -l "$pgdata/server.log" -w -t 120 || return

    # prepare_tpch_imcs.sh creates tables, so create its database here if needed.
    exists=$("${sql[@]}" -d postgres -A -t -v "db_name=$db_name" <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'db_name';
SQL
    ) || return
    if [[ $exists != 1 ]]; then
        "${sql[@]}" -d postgres -v "db_name=$db_name" <<'SQL' || return
CREATE DATABASE :"db_name";
SQL
    fi
    echo "Database: $db_name; port: $port; data directory: $pgdata"
    echo "Next: prepare_tpch_imcs.sh, then run_imcs_queries.sh."
}

case "${1:---help}" in
    build) build ;;
    init) init ;;
    configure) configure ;;
    start) start ;;
    all) build && init && start ;;
    -h|--help)
        echo "Usage: $0 build|init|configure|start|all"
        echo 'build: compile and install; init: gs_initdb; start: start/restart and create DB_NAME.'
        echo 'configure: back up and replace postgresql.conf from REFERENCE_CONF, without restarting.'
        echo 'all: build -> init -> start. Initialization prompts for an administrator password.'
        echo 'Defaults: PGDATA=/mnt/nvme1p1/gauss PORT=19999 DB_NAME=tpch JOBS=8 DOP=192'
        echo 'REFERENCE_CONF defaults to config/imcs_reference.conf (the data instance baseline).'
        echo 'All baseline settings are retained except PORT, the target node name, and Stream sizing.'
        echo 'THREAD_POOL_STREAM_ATTR defaults to "DOP * 5,0.2"; query DOP is set by run_imcs_queries.sh.'
        echo 'NUMACTL_ARGS defaults to "--physcpubind=0-191 --membind=0-7"; set it to empty to disable.'
        echo 'Changing DOP/NUMACTL_ARGS for an experiment requires start and then IMCS populate again.'
        echo 'Paths: SOURCE_DIR, BUILD_DIR, INSTALL_DIR, BINARYLIBS, GCC_PATH.'
        echo 'prepare_tpch_imcs.sh still uses the parent shell PATH or GSQL_BIN.'
        ;;
    *) echo "Unknown step: $1" >&2; exit 1 ;;
esac
