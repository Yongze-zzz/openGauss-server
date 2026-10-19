#!/bin/bash
# Usage: ./build_test_imcs.sh build|init|start|all
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
imcs_cache=${IMCS_CACHE:-256GB}
max_memory=${MAX_PROCESS_MEMORY:-600GB}
shared_buffers=${SHARED_BUFFERS:-50GB}
work_mem=${WORK_MEM:-50GB}
max_connections=${MAX_CONNECTIONS:-4096}
thread_pool_attr=${THREAD_POOL_ATTR:-"192,1,(nobind)"}
thread_pool_stream_attr=${THREAD_POOL_STREAM_ATTR:-"192,0.2,1,(nobind)"}
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

start() {
    local exists
    stop || return

    # Write the common benchmark settings to PGDATA/postgresql.conf before startup.
    # One unbound group leaves CPU placement to the NUMA-aware scan experiment.
    # Scan switches, DOP, CPU lists and populate settings stay in their SQL sessions.
    "$install_dir/bin/gs_guc" set -D "$pgdata" \
        -c "port=$port" -c "max_imcs_cache=$imcs_cache" \
        -c "max_process_memory=$max_memory" \
        -c "shared_buffers=$shared_buffers" -c "work_mem=$work_mem" \
        -c "max_connections=$max_connections" -c "enable_thread_pool=on" \
        -c "thread_pool_attr='$thread_pool_attr'" \
        -c "thread_pool_stream_attr='$thread_pool_stream_attr'" \
        -c "use_workload_manager=off" || return

    # Use this installation, without reusing an old postmaster.opts command.
    "$ctl" start -D "$pgdata" -Z single_node -p "$install_dir/bin/gaussdb" \
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
    start) start ;;
    all) build && init && start ;;
    -h|--help)
        echo "Usage: $0 build|init|start|all"
        echo 'build: compile and install; init: gs_initdb; start: start/restart and create DB_NAME.'
        echo 'all: build -> init -> start. Initialization prompts for an administrator password.'
        echo 'Defaults: PGDATA=/mnt/nvme1p1/gauss PORT=19999 DB_NAME=tpch IMCS_CACHE=256GB JOBS=8'
        echo 'MAX_PROCESS_MEMORY defaults to 600GB, matching the existing test instance.'
        echo 'Memory defaults: SHARED_BUFFERS=50GB WORK_MEM=1GB; MAX_CONNECTIONS=4096.'
        echo 'Thread defaults: THREAD_POOL_ATTR="192,1,(nobind)" THREAD_POOL_STREAM_ATTR="192,0.2,1,(nobind)".'
        echo 'start writes these settings to PGDATA/postgresql.conf and disables workload management.'
        echo 'Paths: SOURCE_DIR, BUILD_DIR, INSTALL_DIR, BINARYLIBS, GCC_PATH.'
        echo 'prepare_tpch_imcs.sh still uses the parent shell PATH or GSQL_BIN.'
        ;;
    *) echo "Unknown step: $1" >&2; exit 1 ;;
esac
