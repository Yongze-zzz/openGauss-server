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
--     ps_partkey,
--     sum(ps_supplycost * ps_availqty) as value
-- from
--     partsupp,
--     supplier,
--     nation
-- where
--     ps_suppkey = s_suppkey
--     and s_nationkey = n_nationkey
--     and n_name = 'GERMANY'
-- group by
--     ps_partkey having
--     sum(ps_supplycost * ps_availqty) > (
--         select
--         sum(ps_supplycost * ps_availqty) * 0.000002
--         from
--             partsupp,
--             supplier,
--             nation
--         where
--             ps_suppkey = s_suppkey
--             and s_nationkey = n_nationkey
--             and n_name = 'GERMANY'
--     )
-- order by
--     value desc;

select /*+
    leading(((n s) ps))
    hashjoin(n s)
    hashjoin(n s ps)
    no nestloop(n s)
    no nestloop(n s ps)
    no mergejoin(n s)
    no mergejoin(n s ps)

    rows(n s #40000)
    rows(n s ps #3200000)
*/
    ps.ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) as value
from
    partsupp ps,
    supplier s,
    nation n
where
    ps.ps_suppkey = s.s_suppkey
    and s.s_nationkey = n.n_nationkey
    and n.n_name = 'GERMANY'
group by
    ps.ps_partkey
having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
        select /*+
            leading(((n2 s2) ps2))
            hashjoin(n2 s2)
            hashjoin(n2 s2 ps2)
            no nestloop(n2 s2)
            no nestloop(n2 s2 ps2)
            no mergejoin(n2 s2)
            no mergejoin(n2 s2 ps2)

            rows(n2 s2 #40000)
            rows(n2 s2 ps2 #3200000)
        */
            sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.000002
        from
            partsupp ps2,
            supplier s2,
            nation n2
        where
            ps2.ps_suppkey = s2.s_suppkey
            and s2.s_nationkey = n2.n_nationkey
            and n2.n_name = 'GERMANY'
    )
order by
    value desc;