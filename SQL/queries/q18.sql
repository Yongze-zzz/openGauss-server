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
--     c_name,
--     c_custkey,
--     o_orderkey,
--     o_orderdate,
--     o_totalprice,
--     sum(l_quantity)
-- from
--     customer,
--     orders,
--     lineitem
-- where
--     o_orderkey  in  (
--         select
--             l_orderkey
--         from
--             lineitem
--         group  by
--             l_orderkey  having
--                 sum(l_quantity)  >  300
--     )
--     and  c_custkey  =  o_custkey
--     and  o_orderkey  =  l_orderkey
-- group  by
--     c_name,
--     c_custkey,
--     o_orderkey,
--     o_orderdate,
--     o_totalprice
-- order  by
--     o_totalprice  desc,
--     o_orderdate
-- limit  100;

select /*+
    leading((((lo o) c) l))

    hashjoin(lo o)
    hashjoin(lo o c)
    hashjoin(lo o c l)

    no nestloop(lo o)
    no nestloop(lo o c)
    no nestloop(lo o c l)

    no mergejoin(lo o)
    no mergejoin(lo o c)
    no mergejoin(lo o c l)

    no indexscan(o)
    no indexscan(l)
    no indexscan(c)

    rows(lo #6398)
    rows(lo o #6398)
    rows(lo o c #6398)
    rows(lo o c l #44786)
*/
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    sum(l.l_quantity)
from
    customer c,
    orders o,
    lineitem l,
    (
        select
            l2.l_orderkey
        from
            lineitem l2
        group by
            l2.l_orderkey
        having
            sum(l2.l_quantity) > 300
    ) lo
where
    o.o_orderkey = lo.l_orderkey
    and c.c_custkey = o.o_custkey
    and o.o_orderkey = l.l_orderkey
group by
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
order by
    o.o_totalprice desc,
    o.o_orderdate
limit 100;