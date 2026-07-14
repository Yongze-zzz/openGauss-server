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

-- Modified

-- select
--     sum(l_extendedprice) / 7.0 as avg_yearly
-- from
--     lineitem,
--     part
-- where
--     p_partkey = l_partkey
--     and p_brand = 'Brand#23'
--     and p_container = 'MED BOX'
--     and l_quantity < (
--         select
--             0.2 * avg(l_quantity)
--         from
--             lineitem
--         where
--             l_partkey = p_partkey
--     );

select /*+
    leading(((p l) a))
    hashjoin(p l)
    hashjoin(p l a)
    no nestloop(p l)
    no nestloop(p l a)
    no mergejoin(p l)
    no mergejoin(p l a)
    no indexscan(p)
    no indexscan(l)

    rows(p #20031)
    rows(p l #600982)
    rows(p l a #53843)
*/
    sum(l.l_extendedprice) / 7.0 as avg_yearly
from
    (
        select
            p_partkey
        from part
        where p_brand = 'Brand#23'
          and p_container = 'MED BOX'
    ) p,
    lineitem l,
    (
        select /*+
            leading((p2 l2))
            hashjoin(p2 l2)
            no nestloop(p2 l2)
            no mergejoin(p2 l2)
            no indexscan(p2)
            no indexscan(l2)

            rows(p2 #20031)
            rows(p2 l2 #600982)
        */
            l2.l_partkey,
            avg(l2.l_quantity) as avg_quantity
        from
            (
                select
                    p_partkey
                from part
                where p_brand = 'Brand#23'
                  and p_container = 'MED BOX'
            ) p2,
            lineitem l2
        where
            l2.l_partkey = p2.p_partkey
        group by
            l2.l_partkey
    ) a
where
    p.p_partkey = l.l_partkey
    and l.l_partkey = a.l_partkey
    and l.l_quantity < 0.2 * a.avg_quantity;