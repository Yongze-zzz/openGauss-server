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
--     cntrycode,
--     count(*) as numcust,
--     sum(c_acctbal) as totacctbal
-- from
--     (
--         select
--             substring(c_phone, 1, 2) as cntrycode,
--             c_acctbal
--         from
--             customer
--         where
--             substring(c_phone, 1, 2) in
--             ('13', '31', '23', '29', '30', '18', '17')
--             and c_acctbal > (
--                 select
--                     avg(c_acctbal)
--                 from
--                     customer
--                 where
--                     c_acctbal > 0.00
--                     and substring(c_phone, 1, 2) in
--                       ('13', '31', '23', '29', '30', '18', '17')
--             )
--             and not exists (
--                 select
--                     *
--                 from
--                     orders
--                 where
--                     o_custkey = c_custkey
--             )
--     ) as custsale
-- group by
--     cntrycode
-- order by
--     cntrycode;
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select /*+
            leading(((c a) oc))
            nestloop(c a)
            hashjoin(c a oc)
            no nestloop(c a oc)
            no mergejoin(c a oc)
            no indexscan(c)
            no indexscan(oc)
            rows(a #1)
            rows(c #4201381)
            rows(c a #1909668)
            rows(oc #9999832)
            rows(c a oc #636749)
        */
            substring(c.c_phone, 1, 2) as cntrycode,
            c.c_acctbal
        from
            customer c
            cross join (
                select /*+
                    no indexscan(ca)
                    rows(ca #3819172)
                */
                    avg(ca.c_acctbal) as avg_acctbal
                from
                    customer ca
                where
                    ca.c_acctbal > 0.00
                    and substring(ca.c_phone, 1, 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
            ) a
            left join (
                select /*+
                    no indexscan(o)
                    rows(o #150000000)
                */
                    o.o_custkey
                from
                    orders o
                group by
                    o.o_custkey
            ) oc
                on oc.o_custkey = c.c_custkey
        where
            substring(c.c_phone, 1, 2) in
                ('13', '31', '23', '29', '30', '18', '17')
            and c.c_acctbal > a.avg_acctbal
            and oc.o_custkey is null
    ) custsale
group by
    cntrycode
order by
    cntrycode;