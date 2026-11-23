SET enable_sonic_hashjoin=on;
SET enable_radix_hashjoin=on;
SET radix_cache_aware=off;
SET radix_chiplet_aware=off;
SET radix_partition_size_mb=8;
SET radix_shared_build=on;
SET radix_shared_direct_send=off;
SET radix_shared_preselect_builders=off;
SET enable_cache_trace=off; SET cache_trace_interval_us=100000;
SET imcs_agg_cache_slots=0;
SET enable_imcs_agg_cache_window=off;
SET enable_imcs_agg_cache_borrow=off;
SET search_path TO :"schema";
SET statement_timeout = 600000;
SET work_mem = :'work_mem';
SET enable_vector_engine = on;
SET enable_codegen = on;
SET enable_imcsscan = on;
SET enable_cstore_partition_lane_scan = off;
SET query_dop = 168;
SET enable_imcs_partition_scan = on;
SET enable_numa_aware_partition_scan = on;
SET enable_imcs_nps = on;
SET enable_imcs_stream_buffer_locality = on;
SET enable_imcs_stream_batch_ring = off;
SET enable_imcs_stream_consumer_placement = off;
SET enable_imcs_stream_hotflow_colocation = off;
SET enable_imcs_stream_path_rebalance = off;
SET enable_imcs_cache_coord = off;
SET enable_imcs_cache_probe_coord = off;
SET imcs_cache_coord_sample_interval = 4;
SET imcs_partition_numa_map = '';
SET imcs_nps_groups = :'query_groups';
SET imcs_partition_scan_cpus = :'cpus';
SET imcs_adapt_period_ms = 1000;
SET enable_imcs_adapt = off;
SET enable_imcs_adapt_migration = off;
SET enable_imcs_local_task_queue = off;
SET enable_intra_operator_data_access = off;
SET enable_inter_operator_data_access = off;
SHOW statement_timeout;
SHOW work_mem;
SHOW query_mem;
SHOW query_max_mem;
SHOW enable_imcs_nps;
SHOW enable_imcs_stream_buffer_locality;
SHOW enable_imcs_stream_batch_ring;
SHOW enable_imcs_stream_consumer_placement;
SHOW enable_imcs_stream_hotflow_colocation;
SHOW enable_imcs_stream_path_rebalance;
SHOW enable_imcs_cache_coord;
SHOW enable_imcs_cache_probe_coord;
SHOW imcs_cache_coord_sample_interval;
SHOW enable_imcs_adapt;
SHOW enable_imcs_adapt_migration;
SHOW enable_imcs_local_task_queue;
SHOW imcs_nps_groups;
SHOW imcs_partition_scan_cpus;
SHOW imcs_adapt_period_ms;
SHOW enable_intra_operator_data_access;
SHOW enable_inter_operator_data_access;
SHOW query_dop;
SHOW enable_codegen;
SHOW enable_imcsscan;
SHOW enable_imcs_partition_scan;
SHOW enable_numa_aware_partition_scan;
SHOW numa_distribute_mode;
SHOW thread_pool_attr;
SHOW thread_pool_stream_attr;
EXPLAIN PERFORMANCE
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- select /*+
--     leading((((((region nation) customer) orders) lineitem) supplier))
--     no nestloop(customer orders lineitem supplier nation region)
--     no nestloop(customer orders lineitem nation region)
--     no indexscan(lineitem)
-- */
--     n_name,
--     sum(l_extendedprice * (1 - l_discount)) as revenue
-- from
--     customer,
--     orders,
--     lineitem,
--     supplier,
--     nation,
--     region
-- where
--     c_custkey = o_custkey
--     and l_orderkey = o_orderkey
--     and l_suppkey = s_suppkey
--     and c_nationkey = s_nationkey
--     and s_nationkey = n_nationkey
--     and n_regionkey = r_regionkey
--     and r_name = 'ASIA'
--     and o_orderdate >= date '1994-01-01'
--     and o_orderdate < date '1994-01-01' + interval '1' year
-- group by
--     n_name
-- order by
--     revenue desc;

-- set enable_nestloop = off;
-- set enable_mergejoin = off;
-- set enable_hashjoin = on;
select /*+
    leading((n r))
    leading((c (n r)))
    leading((o (c (n r))))
    leading((l (o (c (n r)))))
    leading((l (o (c (n r))) s))
    hashjoin(c n r)
    hashjoin(o c n r)
    hashjoin(l o c n r)
    hashjoin(s l o c n r)
    no nestloop(c n r)
    no nestloop(o c n r)
    no nestloop(l o c n r)
    no nestloop(s l o c n r)
    no indexscan(l)
*/
    n.n_name,
    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
from
    customer c,
    orders o,
    lineitem l,
    supplier s,
    nation n,
    region r
where
    c.c_custkey = o.o_custkey
    and l.l_orderkey = o.o_orderkey
    and l.l_suppkey = s.s_suppkey
    and c.c_nationkey = s.s_nationkey
    and s.s_nationkey = n.n_nationkey
    and n.n_regionkey = r.r_regionkey
    and r.r_name = 'ASIA'
    and o.o_orderdate >= date '1994-01-01'
    and o.o_orderdate < date '1994-01-01' + interval '1' year
group by
    n.n_name
order by
    revenue desc;