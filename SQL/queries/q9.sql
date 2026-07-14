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
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select /*+
            leading(((((p ps) (s n)) l) o))
            hashjoin(p ps)
            hashjoin(s n)
            hashjoin(p ps s n)
            hashjoin(p ps s n l)
            hashjoin(p ps s n l o)
            no nestloop(p ps)
            no nestloop(s n)
            no nestloop(p ps s n)
            no nestloop(p ps s n l)
            no nestloop(p ps s n l o)
            rows(ps s *34)
            rows(l ps *32636547)
            rows(l o *32636547)
        */
            n.n_name as nation,
            extract(year from o.o_orderdate) as o_year,
            l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
        from
            part p,
            supplier s,
            lineitem l,
            partsupp ps,
            orders o,
            nation n
        where
            s.s_suppkey = l.l_suppkey
            and ps.ps_suppkey = l.l_suppkey
            and ps.ps_partkey = l.l_partkey
            and p.p_partkey = l.l_partkey
            and o.o_orderkey = l.l_orderkey
            and s.s_nationkey = n.n_nationkey
            and p.p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
