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
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select /*+
            leading((((p l) (((r n1) c) o)) (s n2)))
            hashjoin(p l)
            hashjoin(r n1)
            hashjoin(r n1 c)
            hashjoin(r n1 c o)
            hashjoin(p l r n1 c o)
            hashjoin(s n2)
            hashjoin(p l r n1 c o s n2)
            no nestloop(p l)
            no nestloop(r n1)
            no nestloop(r n1 c)
            no nestloop(r n1 c o)
            no nestloop(p l r n1 c o)
            no nestloop(s n2)
            no nestloop(p l r n1 c o s n2)
            rows(l o *2000)
            rows(l p o c n1 #244821)
        */
            extract(year from o.o_orderdate) as o_year,
            l.l_extendedprice * (1 - l.l_discount) as volume,
            n2.n_name as nation
        from
            part p,
            supplier s,
            lineitem l,
            orders o,
            customer c,
            nation n1,
            nation n2,
            region r
        where
            p.p_partkey = l.l_partkey
            and s.s_suppkey = l.l_suppkey
            and l.l_orderkey = o.o_orderkey
            and o.o_custkey = c.c_custkey
            and c.c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r.r_regionkey
            and r.r_name = 'AMERICA'
            and s.s_nationkey = n2.n_nationkey
            and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p.p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;
