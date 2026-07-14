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