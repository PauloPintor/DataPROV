-- Q1.1
select lineorder.lo_extendedprice, lineorder.lo_discount 
from lineorder, date 
where lineorder.lo_orderdate = date.d_datekey and date.d_year = 1993 and lineorder.lo_discount between 1 and 3 and lineorder.lo_quantity < 25

-- Q1.2
select lineorder.lo_extendedprice, lineorder.lo_discount from lineorder, date where lineorder.lo_orderdate = date.d_datekey and date.d_yearmonthnum = 199401 and lineorder.lo_discount between 4 and 6 and lineorder.lo_quantity between 26 and 35

-- Q1.3
select lineorder.lo_extendedprice, lineorder.lo_discount from lineorder, date where lineorder.lo_orderdate = date.d_datekey and date.d_weeknuminyear = 6 and date.d_year = 1994 and lineorder.lo_discount between 5 and 7 and lineorder.lo_quantity between 26 and 35;

-- Q2.1
select date.d_year, part.p_brand1 from lineorder, date, part, supplier where lineorder.lo_orderdate = date.d_datekey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_suppkey = supplier.s_suppkey and part.p_category = 'MFGR#12' and supplier.s_region = 'AMERICA' group by date.d_year, part.p_brand1 order by date.d_year, part.p_brand1

-- Q2.2
select date.d_year, part.p_brand1 from lineorder, date, part, supplier where lineorder.lo_orderdate = date.d_datekey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_suppkey = supplier.s_suppkey and part.p_brand1 between 'MFGR#2221' and 'MFGR#2228' and supplier.s_region = 'ASIA' group by date.d_year, part.p_brand1 order by date.d_year, part.p_brand1

-- Q2.3
select date.d_year, part.p_brand1 from lineorder, date, part, supplier where lineorder.lo_orderdate = date.d_datekey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_suppkey = supplier.s_suppkey and part.p_brand1 = 'MFGR#2221' and supplier.s_region = 'EUROPE' group by date.d_year, part.p_brand1 order by date.d_year, part.p_brand1

-- Q3.1
select customer.c_nation, supplier.s_nation, date.d_year from customer, lineorder, supplier, date where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_orderdate = date.d_datekey and customer.c_region = 'ASIA' and supplier.s_region = 'ASIA' and date.d_year >= 1992 and date.d_year <= 1997 group by customer.c_nation, supplier.s_nation, date.d_year order by date.d_year asc

--Q3.2
select customer.c_city, supplier.s_city, date.d_year from customer, lineorder, supplier, date where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_orderdate = date.d_datekey and customer.c_nation = 'UNITED STATES' and supplier.s_nation = 'UNITED STATES' and date.d_year >= 1992 and date.d_year <= 1997 group by customer.c_city, supplier.s_city, date.d_year order by date.d_year asc

-- Q3.3
select customer.c_city, supplier.s_city, date.d_year from customer, lineorder, supplier, date where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_orderdate = date.d_datekey and (customer.c_city = 'UNITED KI1' or customer.c_city = 'UNITED KI5') and (supplier.s_city = 'UNITED KI1' or supplier.s_city = 'UNITED KI5') and date.d_year >= 1992 and date.d_year <= 1997 group by customer.c_city, supplier.s_city, date.d_year order by date.d_year asc

-- Q3.4
select customer.c_city, supplier.s_city, date.d_year from customer, lineorder, supplier, date where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_orderdate = date.d_datekey and (customer.c_city = 'UNITED KI1' or customer.c_city = 'UNITED KI5') and (supplier.s_city = 'UNITED KI1' or supplier.s_city = 'UNITED KI5') and date.d_yearmonth = 'Dec1997' group by customer.c_city, supplier.s_city, date.d_year order by date.d_year asc

-- Q4.1
select date.d_year, customer.c_nation from date, customer, supplier, part, lineorder where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_orderdate = date.d_datekey and customer.c_region = 'AMERICA' and supplier.s_region = 'AMERICA' and (part.p_mfgr = 'MFGR#1' or part.p_mfgr = 'MFGR#2') group by date.d_year, customer.c_nation order by date.d_year, customer.c_nation

--Q4.2
select date.d_year, supplier.s_nation, part.p_category from date, customer, supplier, part, lineorder where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_orderdate = date.d_datekey and customer.c_region = 'AMERICA' and supplier.s_region = 'AMERICA' and (date.d_year = 1997 or date.d_year = 1998) and (part.p_mfgr = 'MFGR#1' or part.p_mfgr = 'MFGR#2') group by date.d_year, supplier.s_nation, part.p_category order by date.d_year, supplier.s_nation, part.p_category

--Q4.3
select date.d_year, supplier.s_city, part.p_brand1 from date, customer, supplier, part, lineorder where lineorder.lo_custkey = customer.c_custkey and lineorder.lo_suppkey = supplier.s_suppkey and lineorder.lo_partkey = part.p_partkey and lineorder.lo_orderdate = date.d_datekey and customer.c_region = 'AMERICA' and supplier.s_nation = 'UNITED STATES' and (date.d_year = 1997 or date.d_year = 1998) and part.p_category = 'MFGR#14' group by date.d_year, supplier.s_city, part.p_brand1 order by date.d_year, supplier.s_city, part.p_brand1