# This file is to be replaced with Data Prov to retreive the query result with provenance annotations 

# Provenance annotations: it is assumed that the polynomials are expanded:
#   >  "t1 * t1" instead of "t1^2"
#   >  "t1 x t2 + t1 x t3" instead of "t1 x (t2 + t3)"
#   > "*" means scalar multiplication

# Each variable columnsN is always a tuple with 2 values:
#   > a list with the names of all 'regular' columns, i.e., except the aggregation columns. Use the table name  
#     to deal with ambiguous column names. 
#   > a list with the name of the aggregation columns.
#     In this case, the column name is not important. Only the number of aggregations matters.
# The regular columns come first and the aggregation columns must be in the end.
query22_2 = "SELECT cntrycode, sum(c_acctbal) AS totacctbal, STRING_AGG(custsale.prov || ' .sum ' || CAST(c_acctbal as varchar), ' ⊕ ' ORDER BY cntrycode) AS prov FROM (SELECT substring(customer.c_phone from 1 for 2) AS cntrycode, customer.c_acctbal, COALESCE('customer:' || customer.prov || ' ⊗ ' || C1.prov, 'customer:' || customer.prov) || ' ⊗ ' || C0.prov as prov FROM customer LEFT JOIN (SELECT orders.o_custkey, 'orders:' || orders.prov AS prov FROM orders) AS C1 ON customer.c_custkey = C1.o_custkey, (SELECT avg(customer.c_acctbal) AS col0, STRING_AGG('customer:' || customer.prov || ' .avg ' || CAST(customer.c_acctbal as varchar), ' ⊕ ') AS prov FROM customer WHERE customer.c_acctbal > 0.00 AND substring(customer.c_phone from 1 for 2) IN ('22', '34', '24', '26', '28', '15', '21')) AS C0 WHERE substring(customer.c_phone from 1 for 2) IN ('22', '34', '24', '26', '28', '15', '21') AND customer.c_acctbal > C0.col0 AND C1.o_custkey IS NULL) AS custsale GROUP BY cntrycode ORDER BY cntrycode"
columns22_2 = ([], ['totacctbal'])

query22 = "SELECT cntrycode, count(*) AS numcust, STRING_AGG(custsale.prov || ' .count ' || CAST(1 as varchar), ' ⊕ ' ORDER BY cntrycode) AS prov FROM (SELECT substring(customer.c_phone from 1 for 2) AS cntrycode, customer.c_acctbal, COALESCE('customer:' || customer.prov || ' ⊗ ' || C1.prov, 'customer:' || customer.prov) || ' ⊗ ' || C0.prov as prov FROM customer LEFT JOIN (SELECT orders.o_custkey, 'orders:' || orders.prov AS prov FROM orders) AS C1 ON customer.c_custkey = C1.o_custkey, (SELECT avg(customer.c_acctbal) AS col0, STRING_AGG('customer:' || customer.prov || ' .avg ' || CAST(customer.c_acctbal as varchar), ' ⊕ ') AS prov FROM customer WHERE customer.c_acctbal > 0.00 AND substring(customer.c_phone from 1 for 2) IN ('22', '34', '24', '26', '28', '15', '21')) AS C0 WHERE substring(customer.c_phone from 1 for 2) IN ('22', '34', '24', '26', '28', '15', '21') AND customer.c_acctbal > C0.col0 AND C1.o_custkey IS NULL) AS custsale GROUP BY cntrycode ORDER BY cntrycode"
columns22 = ([], ['numcust'])

query22_1 = "SELECT avg(customer.c_acctbal) AS col0, count(*) as contador, REPLACE(STRING_AGG('customer:' || customer.prov || ' .avg ' || CAST(customer.c_acctbal as varchar) || '/avgcnt', ' ⊕ '), 'avgcnt', CAST(COUNT(*) as varchar)) AS prov FROM customer WHERE customer.c_acctbal > 0.00 AND substring(customer.c_phone from 1 for 2) IN ('22', '34', '24', '26', '28', '15', '21')"
columns22_1 = ([],['col0'])

query21 = "SELECT s_name, count(*) AS numwait, sum(contador) contador, STRING_AGG(COALESCE('supplier:' || supplier.prov || ' ⊗ ' || 'lineitem:' || l1.prov || ' ⊗ ' || '(' || nestedT0.prov || ')' || ' ⊗ ' || C1.prov, 'supplier:' || supplier.prov || ' ⊗ ' || 'lineitem:' || l1.prov || ' ⊗ ' || '(' || nestedT0.prov || ')') || ' ⊗ ' || 'orders:' || orders.prov || ' ⊗ ' || 'nation:' || nation.prov || ' .count ' || CAST(1 as varchar), ' ⊕ ' ORDER BY s_name) AS prov FROM supplier, lineitem l1 JOIN (SELECT l1.l_orderkey, l1.l_suppkey, COUNT(*) contador, STRING_AGG(C0.prov, ' ⊕ ' ORDER BY l1.l_orderkey) AS prov FROM lineitem l1 JOIN (SELECT l2.l_orderkey, l2.l_suppkey, 'lineitem:' || l2.prov AS prov FROM lineitem l2) AS C0 ON l1.l_orderkey = C0.l_orderkey AND l1.l_suppkey <> C0.l_suppkey GROUP BY l1.l_orderkey, l1.l_suppkey) AS nestedT0 ON l1.l_orderkey = nestedT0.l_orderkey AND nestedT0.l_suppkey = l1.l_suppkey LEFT JOIN (SELECT l3.l_orderkey, l3.l_suppkey, 'lineitem:' || l3.prov AS prov FROM lineitem l3 WHERE l3.l_receiptdate > l3.l_commitdate) AS C1 ON l1.l_orderkey = C1.l_orderkey AND l1.l_suppkey <> C1.l_suppkey, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND s_nationkey = n_nationkey AND n_name = 'RUSSIA' AND C1.l_orderkey IS NULL AND C1.l_suppkey IS NULL GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"
columns21 = (['s_name'], ['numwait'])

query20 = "SELECT supplier.s_name, supplier.s_address, 'supplier:' || supplier.prov || ' * ' || '(' || C2.prov || ')' || ' * ' || 'nation:' || nation.prov || ' ' AS prov FROM supplier JOIN (SELECT partsupp.ps_suppkey, STRING_AGG('partsupp:' || partsupp.prov || ' * ' || C0.prov || ' * ' || C1.prov, ' + ' ORDER BY partsupp.ps_suppkey) AS prov FROM partsupp JOIN (SELECT part.p_partkey, STRING_AGG('part:' || part.prov, ' + ' ORDER BY part.p_partkey) AS prov FROM part WHERE part.p_name LIKE 'moccasin%' GROUP BY part.p_partkey) AS C0 ON partsupp.ps_partkey = C0.p_partkey, (SELECT 0.5 * sum(lineitem.l_quantity) AS col0, lineitem.l_partkey, lineitem.l_suppkey, STRING_AGG('lineitem:' || lineitem.prov || ' . ' || CAST(0.5 * lineitem.l_quantity as varchar), ' + ') AS prov FROM lineitem WHERE lineitem.l_shipdate >= date '1997-01-01' AND lineitem.l_shipdate < date '1997-01-01' + INTERVAL '1' year GROUP BY lineitem.l_partkey, lineitem.l_suppkey) AS C1 WHERE partsupp.ps_partkey = C1.l_partkey AND partsupp.ps_suppkey = C1.l_suppkey AND partsupp.ps_availqty > C1.col0 GROUP BY partsupp.ps_suppkey) AS C2 ON supplier.s_suppkey = C2.ps_suppkey, nation WHERE supplier.s_nationkey = nation.n_nationkey AND nation.n_name = 'JAPAN' ORDER BY supplier.s_name"
columns20 = (['supplier.s_name', 'supplier.s_address'],[])

query19 = "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue, count(*) contador, STRING_AGG('(' ||'lineitem:' || lineitem.prov || ' ⊗ ' || 'part:' || part.prov|| ')' || ' .sum ' || CAST(l_extendedprice * (1 - l_discount) as varchar), ' ⊕ ') AS prov FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#53' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 7 AND l_quantity <= 7 + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#53' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 17 AND l_quantity <= 17 + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#53' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
columns19 = ([],['revenue'])

query18 = "SELECT customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_orderdate, orders.o_totalprice, sum(lineitem.l_quantity) as sumQy, sum(contador) contador, STRING_AGG('customer:' || customer.prov || ' ⊗ ' || 'orders:' || orders.prov || ' ⊗ ' || '(' || C0.prov || ')' || ' ⊗ ' || 'lineitem:' || lineitem.prov || ' .sum ' || CAST(lineitem.l_quantity as varchar), ' ⊕ ' ORDER BY customer.c_name) AS prov FROM customer, orders JOIN (SELECT lineitem.l_orderkey, COUNT(*) contador, STRING_AGG('lineitem:' || lineitem.prov, ' ⊕ ' ORDER BY lineitem.l_orderkey) AS prov FROM lineitem GROUP BY lineitem.l_orderkey HAVING sum(lineitem.l_quantity) > 245) AS C0 ON orders.o_orderkey = C0.l_orderkey, lineitem WHERE customer.c_custkey = orders.o_custkey AND orders.o_orderkey = lineitem.l_orderkey GROUP BY customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_orderdate, orders.o_totalprice ORDER BY orders.o_totalprice DESC, orders.o_orderdate LIMIT 100"
columns18 = (['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'],['sumqy'])

query17 = "SELECT sum(lineitem.l_extendedprice) / 7.0 AS avg_yearly, SUM(contador) contador, STRING_AGG('lineitem:' || lineitem.prov || ' ⊗ ' || 'part:' || part.prov|| ' ⊗ ' || '(' || C0.prov || ')' || ' . ' || CAST(lineitem.l_extendedprice/7.0 as varchar), ' ⊕ ') AS prov FROM lineitem, part, (SELECT 0.2 * avg(lineitem.l_quantity) AS F0, lineitem.l_partkey, COUNT(*) contador, STRING_AGG('lineitem:' || lineitem.prov, ' ⊕ ') AS prov FROM lineitem GROUP BY lineitem.l_partkey) AS C0 WHERE part.p_partkey = lineitem.l_partkey AND part.p_brand = 'Brand#21' AND part.p_container = 'WRAP BAG' AND lineitem.l_partkey = C0.l_partkey AND lineitem.l_quantity < C0.F0"
columns17 = ([],['avg_yearly'])

query17_1 = "SELECT 0.2 * avg(lineitem.l_quantity) AS f0, lineitem.l_partkey, count(*) as contador, REPLACE(STRING_AGG('lineitem:' || lineitem.prov || ' . ' || CAST(0.2 * lineitem.l_quantity as varchar) || '/avgcnt', ' + '), 'avgcnt', CAST(COUNT(*) as varchar)) AS prov FROM lineitem GROUP BY lineitem.l_partkey"
columns17_1 = (['l_partkey'],['f0'])

query16="SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt, COUNT(*) as contador, STRING_AGG(COALESCE('partsupp:' || partsupp.prov || ' ⊗ ' || C0.prov, 'partsupp:' || partsupp.prov) || ' ⊗ ' || 'part:' || part.prov || ' .count ' || CAST(1 as varchar), ' ⊕ ' ORDER BY p_brand) AS prov FROM partsupp LEFT JOIN (SELECT supplier.s_suppkey, 'supplier:' || supplier.prov AS prov FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') AS C0 ON partsupp.ps_suppkey = C0.s_suppkey, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#52' and p_type not like 'STANDARD BRUSHED%' AND p_size IN (44, 31, 46, 26, 49, 29, 34, 38) AND C0.s_suppkey IS NULL GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"
columns16 = (['p_brand', 'p_type', 'p_size'],['supplier_cnt'])

query15 = "SELECT supplier.s_suppkey, supplier.s_name, supplier.s_address, supplier.s_phone, total_revenue, 'supplier:' || supplier.prov || ' * ' || revenue0.prov|| ' * ' || C0.prov AS prov FROM supplier, revenue0, (SELECT max(revenue0.total_revenue) AS F0, STRING_AGG(revenue0.prov || ' .max ' || CAST(revenue0.total_revenue as varchar), ' + ') AS prov FROM revenue0, (SELECT max(revenue0.total_revenue) AS F0 FROM revenue0) AS MinMax WHERE revenue0.total_revenue = MinMax.F0) AS C0 WHERE supplier.s_suppkey = supplier_no AND total_revenue = C0.F0 ORDER BY supplier.s_suppkey"
columns15 = (['s_suppkey', 's_name', 's_address', 's_phone'],[])

query14 = "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue, COUNT(*) as contador, REPLACE(STRING_AGG('(' || 'lineitem:' || lineitem.prov || ' * ' || 'part:' || part.prov || ')' || ' . ' || CAST(100.00 * CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END as varchar) || '/avgcnt', ' + '), 'avgcnt', CAST(sum(l_extendedprice * (1 - l_discount)) as varchar)) AS prov FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= date '1996-10-01' AND l_shipdate < date '1996-10-01' + INTERVAL '1' month"
columns14 = ([],['promo_revenue'])

query13_1 = "SELECT c_custkey, count(o_orderkey) c_count, STRING_AGG('(' ||COALESCE('customer:' || customer.prov || ' * ' || 'orders:' || orders.prov , 'customer:' || customer.prov)|| ')' || (case when o_orderkey is null then '' else (' .count ' || CAST(1 as varchar)) end), ' + ' ORDER BY c_custkey) AS prov FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%deposits%' GROUP BY c_custkey"
columns13_1 = (['c_custkey'],['c_count'])

query13 = "SELECT c_count, count(*) AS custdist, SUM(contador) as contador, STRING_AGG('(' ||c_orders.prov || ')' || ' .count ' || CAST(1 as varchar), ' + ' ORDER BY c_count) AS prov FROM (SELECT c_custkey, count(o_orderkey), count(*) contador, STRING_AGG('(' ||COALESCE('customer:' || customer.prov || ' * ' || 'orders:' || orders.prov , 'customer:' || customer.prov) || ')', ' + ' ORDER BY c_custkey) AS prov FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%deposits%' GROUP BY c_custkey) AS c_orders(c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC"
columns13 = ([],['custdist'])
columns13_exp = ['c_custkey'] 

query12_1 = "SELECT l_shipmode, sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count, COUNT(*) as contador, STRING_AGG('(' ||'orders:' || orders.prov || ' * ' || 'lineitem:' || lineitem.prov|| ')' || ' .sum ' || CAST(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END as varchar), ' + ' ORDER BY l_shipmode) AS prov FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('SHIP', 'MAIL') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= TO_DATE('1996-01-01', 'YYYY-MM-DD') AND l_receiptdate < TO_DATE('1997-01-01', 'YYYY-MM-DD') GROUP BY l_shipmode ORDER BY l_shipmode"
columns12_1 = (['l_shipmode'],['low_line_count'])

query12 = "SELECT l_shipmode, sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, COUNT(*) as contador, STRING_AGG('(' ||'orders:' || orders.prov || ' * ' || 'lineitem:' || lineitem.prov|| ')' || ' .sum ' || CAST(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END as varchar)), ' + ' ORDER BY l_shipmode) AS prov FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('SHIP', 'MAIL') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= TO_DATE('1996-01-01', 'YYYY-MM-DD') AND l_receiptdate < TO_DATE('1997-01-01', 'YYYY-MM-DD') GROUP BY l_shipmode ORDER BY l_shipmode"
columns12 = (['l_shipmode'],['high_line_count'])

query11 = "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value, STRING_AGG('(' ||'partsupp:' || partsupp.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'nation:' || nation.prov|| ')' || ' .sum ' || CAST(ps_supplycost * ps_availqty as varchar), ' + ' ORDER BY ps_partkey) AS prov FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > (SELECT sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0010000000 FROM partsupp AS ps2, supplier AS sup2, nation AS n2 WHERE ps2.ps_suppkey = sup2.s_suppkey AND sup2.s_nationkey = n2.n_nationkey AND n2.n_name = 'SAUDI ARABIA') ORDER BY value DESC"
columns11 = (['ps_partkey'],['value'])

query10 = "SELECT c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment, sum(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) contador, STRING_AGG('(' || 'customer:' || customer.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'nation:' || nation.prov || ')' || ' .sum ' || CAST(l_extendedprice * (1 - l_discount) as varchar), ' + ' ORDER BY c_custkey) AS prov FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= TO_DATE('1993-11-01', 'YYYY-MM-DD') AND o_orderdate < TO_DATE('1994-02-01', 'YYYY-MM-DD') AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"
columns10 = (['c_custkey', 'c_name', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment'],['revenue'])

query9 = "SELECT nation, EXTRACT(year FROM o_orderdate) o_year, sum(amount) AS sum_profit, COUNT(*) contador, STRING_AGG('(' ||profit.prov || ')' || ' .sum ' || CAST(amount as varchar), ' + ' ORDER BY nation) AS prov FROM (SELECT n_name AS nation, o_orderdate, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount, 'part:' || part.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'partsupp:' || partsupp.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'nation:' || nation.prov AS prov FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%burlywood%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"
columns9 = (['nation','o_year'],['sum_profit']) 
columns9_exp = ['n_name', 'EXTRACT(year FROM o_orderdate) o_year'] 

query8 = "select o_year, count(*) contador, sum(case when nation = 'IRAN' then volume else 0 end) / sum(volume) as mkt_share, REPLACE(STRING_AGG('(' || all_nations.prov || ')' || ' . ' || CAST(CASE WHEN nation = 'IRAN' THEN volume ELSE 0 END as varchar) || '/avgcnt', ' + ' ORDER BY o_year), 'avgcnt', CAST(sum(volume) as varchar)) AS prov from (select TO_CHAR(o_orderdate, 'YYYY') as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation, 'part:' || part.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'customer:' || customer.prov || ' * ' || 'nation:' || n1.prov || ' * ' || 'nation:' || n2.prov || ' * ' || 'region:' || region.prov as prov from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and s_nationkey = n2.n_nationkey and o_orderdate >= TO_DATE('1995-01-01', 'YYYY-MM-DD') and o_orderdate <= '1996-12-31' and p_type = 'STANDARD POLISHED STEEL') as all_nations group by o_year order by o_year"
columns8 = (['o_year'],['mkt_share']) 
columns8_exp = ["TO_CHAR(o_orderdate, 'YYYY')"] 

query8_2 = "select TO_CHAR(o_orderdate, 'YYYY') as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation, 'part:' || part.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'customer:' || customer.prov || ' * ' || 'nation:' || n1.prov || ' * ' || 'nation:' || n2.prov || ' * ' || 'region:' || region.prov || ' ' as prov from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and s_nationkey = n2.n_nationkey and o_orderdate >= TO_DATE('1995-01-01', 'YYYY-MM-DD') and o_orderdate <= '1996-12-31' and p_type = 'STANDARD POLISHED STEEL'"
columns8_2 = (["o_year", 'volume', 'nation'],[])
columns8_2_exp = ["TO_CHAR(o_orderdate, 'YYYY')", 'l_extendedprice * (1 - l_discount)', 'n3.n_name'] 

query7 = "SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue, COUNT(shipping.supp_nation) contador, STRING_AGG('(' || shipping.prov || ')' || ' .sum ' || CAST(volume as varchar), ' + ' ORDER BY supp_nation) AS prov FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(year FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume, 'supplier:' || supplier.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'customer:' || customer.prov || ' * ' || 'nation:' || n1.prov || ' * ' || 'nation:' || n2.prov AS prov FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'RUSSIA' AND n2.n_name = 'IRAN') OR (n1.n_name = 'IRAN' AND n2.n_name = 'RUSSIA')) AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"
columns7 = (['supp_nation', 'cust_nation', 'l_year'],['revenue']) 
columns7_exp = ['nation.n_name', 'n3.n_name', 'EXTRACT(year FROM l_shipdate) l_year']

query6 = "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) contador, STRING_AGG('lineitem:' || lineitem.prov || ' .sum ' || CAST(l_extendedprice * l_discount as varchar), ' + ') AS prov FROM lineitem WHERE l_shipdate >= TO_DATE('1993-01-01', 'YYYY-MM-DD') AND l_shipdate < TO_DATE('1994-01-01', 'YYYY-MM-DD') AND l_discount >= 0.08 AND l_discount < 0.1 AND l_quantity < 24"
columns6 = ([],['revenue']) 

query5 ="SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, count(*) contador, STRING_AGG('(' ||'customer:' || customer.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'lineitem:' || lineitem.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'nation:' || nation.prov || ' * ' || 'region:' || region.prov|| ')' || ' .sum ' || CAST(l_extendedprice * (1 - l_discount) as varchar), ' + ' ORDER BY n_name) AS prov FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND o_orderdate >= TO_DATE('1993-01-01', 'YYYY-MM-DD') AND o_orderdate < TO_DATE('1994-01-01', 'YYYY-MM-DD') GROUP BY n_name ORDER BY revenue DESC"
columns5 = (['n_name'],['revenue']) 

query4="SELECT orders.o_orderpriority, count(*) AS order_count, SUM(contador) as contador, STRING_AGG('orders:' || orders.prov || ' * ' || '(' || nestedT0.prov || ')' || ' .count ' || CAST(1 as varchar), ' + ' ORDER BY orders.o_orderpriority) AS prov FROM orders JOIN (SELECT orders.o_orderkey, COUNT(*) contador, STRING_AGG(C0.prov , ' + ' ORDER BY orders.o_orderkey) AS prov FROM orders JOIN (SELECT lineitem.l_orderkey, 'lineitem:' || lineitem.prov AS prov FROM lineitem WHERE lineitem.l_commitdate < lineitem.l_receiptdate) AS C0 ON orders.o_orderkey = C0.l_orderkey GROUP BY orders.o_orderkey) AS nestedT0 ON orders.o_orderkey = nestedT0.o_orderkey WHERE orders.o_orderdate >= TO_DATE('1995-08-01', 'YYYY-MM-DD') AND orders.o_orderdate < TO_DATE('1995-11-01', 'YYYY-MM-DD') GROUP BY orders.o_orderpriority ORDER BY orders.o_orderpriority"
columns4 = (['o_orderpriority'],['order_count']) 

query3="SELECT l_orderkey, o_orderdate, o_shippriority, sum(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) AS contador, STRING_AGG('(' ||'customer:' || customer.prov || ' * ' || 'orders:' || orders.prov || ' * ' || 'lineitem:' || lineitem.prov|| ')' || ' .sum ' || CAST(l_extendedprice * (1 - l_discount) as varchar), ' + ' ORDER BY l_orderkey) AS prov FROM customer, orders, lineitem WHERE c_mktsegment = 'FURNITURE' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < TO_DATE('1995-03-17', 'YYYY-MM-DD') AND l_shipdate > TO_DATE('1995-03-17', 'YYYY-MM-DD') GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
columns3 = (['l_orderkey', 'o_orderdate', 'o_shippriority'],['revenue']) 

query2_1="SELECT partsupp.ps_partkey, min(partsupp.ps_supplycost) F0, count(*) contador, STRING_AGG('(' ||'partsupp:' || partsupp.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'nation:' || nation.prov || ' * ' || 'region:' || region.prov|| ')' || ' .min ' || CAST(partsupp.ps_supplycost as varchar), ' + ') AS prov FROM partsupp, supplier, nation, region, (SELECT min(partsupp.ps_supplycost) F0, partsupp.ps_partkey FROM partsupp, supplier, nation, region WHERE supplier.s_suppkey = partsupp.ps_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA' GROUP BY partsupp.ps_partkey) AS MinMax WHERE supplier.s_suppkey = partsupp.ps_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA' AND partsupp.ps_supplycost = MinMax.F0 AND partsupp.ps_partkey = MinMax.ps_partkey GROUP BY partsupp.ps_partkey"

columns2_1 = (['ps_partkey'],['f0']) 

query2 = "SELECT supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment, C0.contador, 'part:' || part.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'partsupp:' || partsupp.prov || ' * ' || 'nation:' || nation.prov || ' * ' || 'region:' || region.prov || ' * ' || C0.prov AS prov FROM part, supplier, partsupp, nation, region, (SELECT min(partsupp.ps_supplycost) AS F0, partsupp.ps_partkey, STRING_AGG('(' || 'partsupp:' || partsupp.prov || ' * ' || 'supplier:' || supplier.prov || ' * ' || 'nation:' || nation.prov || ' * ' || 'region:' || region.prov || ')' || ' * ' || CAST(partsupp.ps_supplycost as varchar), ' + ') AS prov FROM partsupp, supplier, nation, region, (SELECT min(partsupp.ps_supplycost) AS F0, COUNT(*) contador, partsupp.ps_partkey FROM partsupp, supplier, nation, region WHERE supplier.s_suppkey = partsupp.ps_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA' GROUP BY partsupp.ps_partkey) AS MinMax WHERE supplier.s_suppkey = partsupp.ps_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA' AND partsupp.ps_supplycost = MinMax.F0 AND partsupp.ps_partkey = MinMax.ps_partkey GROUP BY partsupp.ps_partkey) AS C0 WHERE part.p_partkey = partsupp.ps_partkey AND supplier.s_suppkey = partsupp.ps_suppkey AND part.p_size = 33 and part.p_type like '%BRASS' AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA' AND part.p_partkey = C0.ps_partkey AND partsupp.ps_supplycost = C0.F0 ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey LIMIT 100"
columns2 = (['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment'],[]) 


query1_2 = "SELECT sum(l_quantity) AS sum_qty, STRING_AGG('lineitem:' || lineitem.prov || ' .sum ' || CAST(l_quantity as varchar), ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_2 = ([],['sum_qty'])

query1_3 = "SELECT sum(l_extendedprice) AS sum_base_price, STRING_AGG('lineitem:' || lineitem.prov || ' .sum ' || CAST(l_extendedprice as varchar), ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_3 = ([],['sum_base_price'])

query1_4 = "SELECT sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, STRING_AGG('lineitem:' || lineitem.prov || ' .sum ' || CAST(l_extendedprice * (1 - l_discount) as varchar), ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_4 = ([],['sum_disc_price'])

query1_5 = "SELECT sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, STRING_AGG('lineitem:' || lineitem.prov || CAST(l_extendedprice * (1 - l_discount) * (1 + l_tax) as varchar), ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_5 = ([],['sum_charge'])

query1_6 = "SELECT avg(l1.l_quantity) AS avg_qty, REPLACE(STRING_AGG('lineitem:' || l1.prov || ' .avg ' || CAST(l_quantity as varchar) || '/avgcnt', ' ⊕ ' ORDER BY l_returnflag), 'avgcnt', CAST(COUNT(l1.l_quantity) as varchar)) AS prov FROM lineitem l1 WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"
columns1_6 = ([],['avg_qty'])

query1_7 = "SELECT avg(l_discount) AS avg_disc, count(*) AS count_order, REPLACE(STRING_AGG('lineitem:' || lineitem.prov || ' .avg ' || CAST(l_discount as varchar) || '/avgcnt', ' ⊕ ' ORDER BY l_returnflag), 'avgcnt', CAST(COUNT(l1.l_discount) as varchar)) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_7 = ([],['avg_disc'])	

query1_8 = "SELECT count(*) AS count_order, STRING_AGG('lineitem:' || lineitem.prov || ' .count ' || CAST(1 as varchar), ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1_8 = ([],['count_order'])

query1 ="SELECT l_returnflag, l_linestatus,  STRING_AGG('lineitem:' || lineitem.prov, ' ⊕ ' ORDER BY l_returnflag) AS prov FROM lineitem WHERE l_shipdate <= TO_DATE('1998-09-14', 'YYYY-MM-DD') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
columns1 = (['l_returnflag', 'l_linestatus'],[])

# my functions
def getQuery(num):
	if num == 1:
		return query1, columns1, []
	elif num == 2:
		return query1_2, columns1_2, []
	elif num == 3:
		return query1_3, columns1_3, []
	elif num == 4:
		return query1_4, columns1_4, []
	elif num == 5:
		return query1_5, columns1_5, []
	elif num == 6:
		return query1_6, columns1_6, []
	elif num == 7:
		return query1_7, columns1_7, []
	elif num == 8:
		return query1_8, columns1_8, []
	elif num == 9:
		return query2, columns2, []
	elif num == 10:
		return query2_1, columns2_1, []
	elif num == 11:
		return query3, columns3, []
	elif num == 12:
		return query4, columns4, []
	elif num == 13:
		return query5, columns5, []
	elif num == 14:
		return query6, columns6, []
	elif num == 15:
		return query7, columns7, columns7_exp
	elif num == 16:
		return query8, columns8, columns8_exp
	elif num == 17:
		return query8_2, columns8_2, columns8_2_exp
	elif num == 18:
		return query9, columns9, columns9_exp
	elif num == 19:
		return query10, columns10, []
	elif num == 20:
		return query11, columns11, []
	elif num == 21:
		return query12_1, columns12_1, []
	elif num == 22:
		return query12, columns12, []
	elif num == 23:
		return query13, columns13, columns13_exp
	elif num == 24:
		return query13_1, columns13_1, []
	elif num == 25:
		return query14, columns14, []
	elif num == 26:
		return query15, columns15, []
	elif num == 27:
		return query16, columns16, []
	elif num == 28:
		return query17, columns17, []
	elif num == 29:
		return query17_1, columns17_1, []
	elif num == 30:
		return query18, columns18, []
	elif num == 31:
		return query19, columns19, []
	elif num == 32:
		return query20, columns20, []
	elif num == 33:
		return query21, columns21, []
	elif num == 34:
		return query22, columns22
	elif num == 35:	
		return query22_1, columns22_1
	elif num == 36:	
		return query22_2, columns22_2
	else:
		return "", []