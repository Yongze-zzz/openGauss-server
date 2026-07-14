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
--     count(*) as numwait
-- from
--     supplier,
--     lineitem l1,
--     orders,
--     nation
-- where
--     s_suppkey = l1.l_suppkey
--     and o_orderkey = l1.l_orderkey
--     and o_orderstatus = 'F'
--     and l1.l_receiptdate > l1.l_commitdate
--     and exists (
--         select
--             *
--         from
--             lineitem l2
--         where
--                 l2.l_orderkey = l1.l_orderkey
--           and l2.l_suppkey <> l1.l_suppkey
--     )
--     and not exists (
--         select
--             *
--         from
--             lineitem l3
--         where
--                 l3.l_orderkey = l1.l_orderkey
--           and l3.l_suppkey <> l1.l_suppkey
--           and l3.l_receiptdate > l3.l_commitdate
--     )
--     and s_nationkey = n_nationkey
--     and n_name = 'SAUDI ARABIA'
-- group by
--     s_name
-- order by
--     numwait desc,
--     s_name
-- limit 100;

select /*+
    leading((((((n s) l1) o) mo) lo))
    hashjoin(n s)
    hashjoin(n s l1)
    hashjoin(n s l1 o)
    hashjoin(n s l1 o mo)
    hashjoin(n s l1 o mo lo)
    no nestloop(n s)
    no nestloop(n s l1)
    no nestloop(n s l1 o)
    no nestloop(n s l1 o mo)
    no nestloop(n s l1 o mo lo)
    no indexscan(s)
    no indexscan(n)
    no indexscan(o)
    no indexscan(l1)
    rows(n #1)
    rows(n s #40000)
    rows(l1 #300000000)
    rows(n s l1 #12000000)
    rows(n s l1 o #6000000)
    rows(mo #145000000)
    rows(lo #50000000)
    rows(n s l1 o mo #6000000)
    rows(n s l1 o mo lo #2000000)
*/
    s.s_name,
    count(*) as numwait
from
    supplier s,
    lineitem l1,
    orders o,
    nation n,
    (
        select /*+
            no indexscan(l2)
            rows(l2 #600000000)
        */
            l2.l_orderkey
        from
            lineitem l2
        group by
            l2.l_orderkey
        having
            min(l2.l_suppkey) <> max(l2.l_suppkey)
    ) mo,
    (
        select /*+
            no indexscan(l3)
            rows(l3 #300000000)
        */
            l3.l_orderkey,
            min(l3.l_suppkey) as only_late_suppkey
        from
            lineitem l3
        where
            l3.l_receiptdate > l3.l_commitdate
        group by
            l3.l_orderkey
        having
            min(l3.l_suppkey) = max(l3.l_suppkey)
    ) lo
where
    s.s_suppkey = l1.l_suppkey
    and o.o_orderkey = l1.l_orderkey
    and o.o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and mo.l_orderkey = l1.l_orderkey
    and lo.l_orderkey = l1.l_orderkey
    and lo.only_late_suppkey = l1.l_suppkey
    and s.s_nationkey = n.n_nationkey
    and n.n_name = 'SAUDI ARABIA'
group by
    s.s_name
order by
    numwait desc,
    s.s_name
limit 100;