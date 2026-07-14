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
--     supp_nation,
--     cust_nation,
--     l_year,
--     sum(volume) as revenue
-- from
--     (
--         select /*+
--             leading((((n1 s) l) ((n2 c) o)))
--             hashjoin(n1 s)
--             hashjoin(n1 s l)
--             hashjoin(n2 c)
--             hashjoin(n2 c o)
--             hashjoin(n1 s l n2 c o)
--             no nestloop(n1 s)
--             no nestloop(n1 s l)
--             no nestloop(n2 c)
--             no nestloop(n2 c o)
--             no nestloop(n1 s l n2 c o)
--         */
--             n1.n_name as supp_nation,
--             n2.n_name as cust_nation,
--             extract(year from l.l_shipdate) as l_year,
--             l.l_extendedprice * (1 - l.l_discount) as volume
--         from
--             supplier s,
--             lineitem l,
--             orders o,
--             customer c,
--             nation n1,
--             nation n2
--         where
--             s.s_suppkey = l.l_suppkey
--             and o.o_orderkey = l.l_orderkey
--             and c.c_custkey = o.o_custkey
--             and s.s_nationkey = n1.n_nationkey
--             and c.c_nationkey = n2.n_nationkey
--             and (
--                 (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
--                 or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
--             )
--             and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
--     ) as shipping
-- group by
--     supp_nation,
--     cust_nation,
--     l_year
-- order by
--     supp_nation,
--     cust_nation,
--     l_year;

select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select /*+
            leading((((n1 s) l) ((n2 c) o)))

            hashjoin(n1 s)
            hashjoin(n1 s l)
            hashjoin(n2 c)
            hashjoin(n2 c o)
            hashjoin(n1 s l n2 c o)

            no nestloop(n1 s)
            no nestloop(n1 s l)
            no nestloop(n2 c)
            no nestloop(n2 c o)
            no nestloop(n1 s l n2 c o)

            rows(n1 s l #182301190)
            rows(n2 c o #150000000)
            rows(n1 s l n2 c o #583512)
        */
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l.l_shipdate) as l_year,
            l.l_extendedprice * (1 - l.l_discount) as volume
        from
            supplier s,
            lineitem l,
            orders o,
            customer c,
            nation n1,
            nation n2
        where
            s.s_suppkey = l.l_suppkey
            and o.o_orderkey = l.l_orderkey
            and c.c_custkey = o.o_custkey
            and s.s_nationkey = n1.n_nationkey
            and c.c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;