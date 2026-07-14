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

select
    q.s_acctbal,
    q.s_name,
    q.n_name,
    q.p_partkey,
    q.p_mfgr,
    q.s_address,
    q.s_phone,
    q.s_comment
from
    (
        select /*+
            leading(((p ps) ((r n) s)))
            hashjoin(p ps)
            hashjoin(r n)
            hashjoin(r n s)
            hashjoin(p ps r n s)
            no nestloop(p ps)
            no nestloop(r n)
            no nestloop(r n s)
            no nestloop(p ps r n s)
        */
            s.s_acctbal,
            s.s_name,
            n.n_name,
            p.p_partkey,
            p.p_mfgr,
            s.s_address,
            s.s_phone,
            s.s_comment,
            ps.ps_supplycost,
            min(ps.ps_supplycost) over (partition by p.p_partkey) as min_supplycost
        from
            part p,
            supplier s,
            partsupp ps,
            nation n,
            region r
        where
            p.p_partkey = ps.ps_partkey
            and s.s_suppkey = ps.ps_suppkey
            and p.p_size = 15
            and p.p_type like '%BRASS'
            and s.s_nationkey = n.n_nationkey
            and n.n_regionkey = r.r_regionkey
            and r.r_name = 'EUROPE'
    ) q
where
    q.ps_supplycost = q.min_supplycost
order by
    q.s_acctbal desc,
    q.n_name,
    q.s_name,
    q.p_partkey
limit 100;
