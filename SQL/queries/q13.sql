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
--     c_count,
--     count(*) as custdist
-- from
--     (
--         select
--             c_custkey,
--             count(o_orderkey) as c_count
--         from
--             customer left outer join orders on
--                 c_custkey = o_custkey
--                 and o_comment not like '%special%requests%'
--         group by
--             c_custkey
--     ) as c_orders
-- group by
--     c_count
-- order by
--     custdist desc,
--     c_count desc;

select
    c_count,
    count(*) as custdist
from
    (
        select /*+
            leading((c o))
            hashjoin(c o)
            no nestloop(c o)
            no mergejoin(c o)
        */
            c.c_custkey,
            count(o.o_orderkey) as c_count
        from
            customer c
            left outer join orders o on
                c.c_custkey = o.o_custkey
                and o.o_comment not like '%special%requests%'
        group by
            c.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;