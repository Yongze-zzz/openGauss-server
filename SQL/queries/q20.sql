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

-- select
--     s_name,
--     s_address
-- from
--     supplier,
--     nation
-- where
--     s_suppkey in (
--         select
--             ps_suppkey
--         from
--             partsupp
--         where
--             ps_partkey in (
--                 select
--                     p_partkey
--                 from
--                     part
--                 where
--                         p_name like 'forest%'
--             )
--             and ps_availqty > (
--                 select
--                     0.5 * sum(l_quantity)
--                 from
--                     lineitem
--                 where
--                     l_partkey = ps_partkey
--                     and l_suppkey = ps_suppkey
--                     and l_shipdate >= date '1994-01-01'
--                     and l_shipdate < date '1994-01-01' + interval '1' year
--             )
--     )
--     and s_nationkey = n_nationkey
--     and n_name = 'CANADA'
-- order by
--     s_name;

select /*+
    leading(((n s) qs))
    hashjoin(n s)
    hashjoin(n s qs)
    no nestloop(n s)
    no nestloop(n s qs)
    no indexscan(s)
    no indexscan(n)
    rows(n #1)
    rows(n s #40000)
    rows(qs #8000)
    rows(n s qs #1000)
*/
    s.s_name,
    s.s_address
from
    supplier s,
    nation n,
    (
        select /*+
            leading(((p ps) lq))
            hashjoin(p ps)
            hashjoin(p ps lq)
            no nestloop(p ps)
            no nestloop(p ps lq)
            no indexscan(p)
            no indexscan(ps)
            rows(p #220000)
            rows(p ps #880000)
            rows(lq #50000000)
            rows(p ps lq #880000)
        */
            ps.ps_suppkey
        from
            part p,
            partsupp ps,
            (
                select /*+
                    no indexscan(l)
                    rows(l #85000000)
                */
                    l.l_partkey,
                    l.l_suppkey,
                    0.5 * sum(l.l_quantity) as half_qty
                from
                    lineitem l
                where
                    l.l_shipdate >= date '1994-01-01'
                    and l.l_shipdate < date '1994-01-01' + interval '1' year
                group by
                    l.l_partkey,
                    l.l_suppkey
            ) lq
        where
            p.p_name like 'forest%'
            and p.p_partkey = ps.ps_partkey
            and lq.l_partkey = ps.ps_partkey
            and lq.l_suppkey = ps.ps_suppkey
            and ps.ps_availqty > lq.half_qty
        group by
            ps.ps_suppkey
    ) qs
where
    s.s_suppkey = qs.ps_suppkey
    and s.s_nationkey = n.n_nationkey
    and n.n_name = 'CANADA'
order by
    s.s_name;