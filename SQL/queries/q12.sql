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
--     l_shipmode,
--     sum(case
--         when o_orderpriority = '1-URGENT'
--             or o_orderpriority = '2-HIGH'
--             then 1
--         else 0
--     end) as high_line_count,
--     sum(case
--         when o_orderpriority <> '1-URGENT'
--             and o_orderpriority <> '2-HIGH'
--             then 1
--         else 0
--     end) as low_line_count
-- from
--     orders,
--     lineitem
-- where
--     o_orderkey = l_orderkey
--     and l_shipmode in ('MAIL', 'SHIP')
--     and l_commitdate < l_receiptdate
--     and l_shipdate < l_commitdate
--     and l_receiptdate >= date '1994-01-01'
--     and l_receiptdate < date '1994-01-01' + interval '1' year
-- group by
--     l_shipmode
-- order by
--     l_shipmode;

select /*+
    leading((l o))
    hashjoin(l o)
    no nestloop(l o)
    no mergejoin(l o)

    rows(l #7600000)
    rows(l o #7600000)
*/
    l.l_shipmode,
    sum(case
        when o.o_orderpriority = '1-URGENT'
            or o.o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o.o_orderpriority <> '1-URGENT'
            and o.o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    orders o,
    lineitem l
where
    o.o_orderkey = l.l_orderkey
    and l.l_shipmode in ('MAIL', 'SHIP')
    and l.l_commitdate < l.l_receiptdate
    and l.l_shipdate < l.l_commitdate
    and l.l_receiptdate >= date '1994-01-01'
    and l.l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l.l_shipmode
order by
    l.l_shipmode;
