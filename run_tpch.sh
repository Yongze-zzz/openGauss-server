#!/bin/bash
# Usage: bash run_tpch.sh
# Build and start the database, then import the existing SF100 data and populate IMCS.


source_dir=$(cd "$(dirname "$0")" && pwd -P)
binarylibs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
export GAUSSHOME=${INSTALL_DIR:-$source_dir/mppdb_temp_install}
export GSQL_BIN="$GAUSSHOME/bin/gsql"
export PATH="$GAUSSHOME/bin:$PATH"
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$binarylibs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
export PGDATA=${PGDATA:-/mnt/nvme1p1/gauss}
export PORT=${PORT:-${PGPORT:-19999}}

# The existing .tbl files are in the dbgen directory. Override DATA_DIR for other data.
export DATA_DIR=${DATA_DIR:-${DBGEN_DIR:-${TPCH_ROOT:-/mnt/nvme1p1/yanyz/TPCH-openGauss-TestTools}/TPC-H_Tools_v3.0.0/dbgen}}

bash "$source_dir/build_test_imcs.sh" all
bash "$source_dir/prepare_tpch_imcs.sh" create
bash "$source_dir/prepare_tpch_imcs.sh" load
bash "$source_dir/prepare_tpch_imcs.sh" populate
bash "$source_dir/prepare_tpch_imcs.sh" status
