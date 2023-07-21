package com.generic.provenance;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.CachedRowSet;
import com.generic.Helpers.TrinoHelper;
import com.generic.Parser.Parser;
import com.generic.ResultProcess.ResultProcess;

public class App 
{
    public static void main( String[] args )
    {	

		String sql = "SELECT ca.tpch1.partsupp.suppkey FROM ca.tpch1.partsupp WHERE ca.tpch1.partsupp.partkey IN ( SELECT ca.tpch1.part.partkey FROM ca.tpch1.part WHERE ca.tpch1.part.name LIKE 'forest%') AND ca.tpch1.partsupp.availqty > (SELECT 0.5 * SUM(ca.tpch1.lineitem.quantity) FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.partkey = ca.tpch1.partsupp.partkey AND ca.tpch1.lineitem.suppkey = ca.tpch1.partsupp.suppkey AND ca.tpch1.lineitem.shipdate >= DATE '1994-1-1' AND ca.tpch1.lineitem.shipdate < DATE '1994-1-1' + interval '1' year)";

sql = "SELECT ca.tpch1.supplier.name, ca.tpch1.supplier.address FROM ca.tpch1.supplier, ca.tpch1.nation WHERE ca.tpch1.supplier.suppkey IN (SELECT ca.tpch1.partsupp.suppkey FROM ca.tpch1.partsupp WHERE ca.tpch1.partsupp.partkey IN ( SELECT ca.tpch1.part.partkey FROM ca.tpch1.part WHERE ca.tpch1.part.name LIKE 'forest%') AND ca.tpch1.partsupp.availqty > (SELECT 0.5 * SUM(ca.tpch1.lineitem.quantity) FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.partkey = ca.tpch1.partsupp.partkey AND ca.tpch1.lineitem.suppkey = ca.tpch1.partsupp.suppkey AND ca.tpch1.lineitem.shipdate >= DATE '1994-1-1' AND ca.tpch1.lineitem.shipdate < DATE '1994-1-1' + interval '1' year)) AND ca.tpch1.supplier.nationkey = ca.tpch1.nation.nationkey AND ca.tpch1.nation.name = 'CANADA' ORDER BY ca.tpch1.supplier.name";
		
        try {
			Parser parser = new Parser();
			String newSQL = parser.parseQuery(sql);

			TrinoHelper th = new TrinoHelper();
            ResultSet result = th.ExecuteQuery(newSQL);
			//CachedRowSet newResult = th.ResultSetToCachedRowSet(result);
            //th.printQueryResult(newResult);
			ResultProcess rp = new ResultProcess();
			rp.processing(result);
			rp.printResult();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}


/*
		//query 3 - TPCH
		sql = "SELECT ca.tpch1.lineitem.orderkey, ca.tpch1.orders.orderdate, ca.tpch1.orders.ship_priority, SUM(ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount)) AS revenue FROM ca.tpch1.customer, ca.tpch1.orders, ca.tpch1.lineitem WHERE ca.tpch1.customer.mktsegment = 'BUILDING' AND ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.orders.orderdate < DATE '1995-3-15' AND ca.tpch1.lineitem.shipdate > DATE '1995-3-15' GROUP BY ca.tpch1.lineitem.orderkey, ca.tpch1.orders.orderdate, ca.tpch1.orders.ship_priority ORDER BY revenue DESC, ca.tpch1.orders.orderdate LIMIT 10";

		//query 4 - TPCH
		sql = "SELECT ca.tpch1.orders.order_priority, COUNT(*) AS order_count FROM ca.tpch1.orders WHERE ca.tpch1.orders.orderdate >= DATE '1993-1-7' AND ca.tpch1.orders.orderdate < DATE '1993-1-7' + interval '3' month AND EXISTS (SELECT * FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.lineitem.commitdate < ca.tpch1.lineitem.receiptdate) GROUP BY ca.tpch1.orders.order_priority ORDER BY ca.tpch1.orders.order_priority";

 * 		//query 5 - TPCH
		sql = "SELECT ca.tpch1.nation.name, SUM(ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount)) AS revenue FROM ca.tpch1.customer, ca.tpch1.orders, ca.tpch1.lineitem, ca.tpch1.supplier, ca.tpch1.nation, ca.tpch1.region WHERE ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.lineitem.suppkey = ca.tpch1.supplier.suppkey AND ca.tpch1.customer.nationkey = ca.tpch1.supplier.nationkey AND ca.tpch1.supplier.nationkey = ca.tpch1.nation.nationkey AND ca.tpch1.nation.regionkey = ca.tpch1.region.regionkey AND ca.tpch1.region.name = 'ASIA' AND ca.tpch1.orders.orderdate >= DATE '1994-1-1' AND ca.tpch1.orders.orderdate < DATE '1994-1-1' + interval '1' year GROUP BY ca.tpch1.nation.name ORDER BY revenue DESC";

		//query 6 - TPCH
		sql = "SELECT SUM(ca.tpch1.lineitem.extendedprice * ca.tpch1.lineitem.discount) AS revenue FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.shipdate >= DATE '1994-1-1' AND ca.tpch1.lineitem.shipdate < DATE '1994-1-1' + interval '1' year AND ca.tpch1.lineitem.discount BETWEEN .06 - 0.01 AND .06 + 0.010001	AND ca.tpch1.lineitem.quantity < 24";

		//query 7 - TPCH
		sql = "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM ( SELECT n1.name AS supp_nation, n2.name AS cust_nation, YEAR(ca.tpch1.lineitem.shipdate) AS l_year, ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount) AS volume FROM ca.tpch1.supplier, ca.tpch1.lineitem, ca.tpch1.orders, ca.tpch1.customer, ca.tpch1.nation n1, ca.tpch1.nation n2 WHERE ca.tpch1.supplier.suppkey = ca.tpch1.lineitem.suppkey AND ca.tpch1.orders.orderkey = ca.tpch1.lineitem.orderkey AND ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.supplier.nationkey = n1.nationkey AND ca.tpch1.customer.nationkey = n2.nationkey AND ( (n1.name = 'FRANCE' AND n2.name = 'GERMANY') OR (n1.name = 'GERMANY' AND n2.name = 'FRANCE') ) AND ca.tpch1.lineitem.shipdate BETWEEN DATE '1995-1-1' AND DATE '1996-12-31' ) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year";

		//query 9 - TPCH
		sql = "SELECT profit.nation, profit.o_year, SUM(profit.amount) AS sum_profit FROM (SELECT ca.tpch1.nation.name AS nation, YEAR(ca.tpch1.orders.orderdate) AS o_year, ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount) - ca.tpch1.partsupp.supplycost * ca.tpch1.lineitem.quantity AS amount FROM ca.tpch1.part, ca.tpch1.supplier, ca.tpch1.lineitem, ca.tpch1.partsupp, ca.tpch1.orders, ca.tpch1.nation WHERE ca.tpch1.supplier.suppkey = ca.tpch1.lineitem.suppkey AND ca.tpch1.partsupp.suppkey = ca.tpch1.lineitem.suppkey AND ca.tpch1.partsupp.partkey = ca.tpch1.lineitem.partkey AND ca.tpch1.part.partkey = ca.tpch1.lineitem.partkey AND ca.tpch1.orders.orderkey = ca.tpch1.lineitem.orderkey AND ca.tpch1.supplier.nationkey = ca.tpch1.nation.nationkey AND ca.tpch1.part.name LIKE '%green%' ) AS profit GROUP BY profit.nation, profit.o_year ORDER BY profit.nation, profit.o_year DESC";

		//query 10 - TPCH
		sql = "SELECT ca.tpch1.customer.custkey, ca.tpch1.customer.name, SUM(ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount)) AS revenue, ca.tpch1.customer.acctbal, ca.tpch1.nation.name, ca.tpch1.customer.address, ca.tpch1.customer.phone, ca.tpch1.customer.comment FROM ca.tpch1.customer, ca.tpch1.orders, ca.tpch1.lineitem, ca.tpch1.nation WHERE ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.orders.orderdate >= DATE '1993-10-1' AND ca.tpch1.orders.orderdate < DATE '1993-10-1' + interval '3' month AND ca.tpch1.lineitem.returnflag = 'R' AND  ca.tpch1.customer.nationkey = ca.tpch1.nation.nationkey GROUP BY ca.tpch1.customer.custkey, ca.tpch1.customer.name, ca.tpch1.customer.acctbal, ca.tpch1.customer.phone, ca.tpch1.nation.name, ca.tpch1.customer.address, ca.tpch1.customer.comment ORDER BY revenue DESC LIMIT 20";

		//query 11 - TPCH
		Falar com o Professor

		//query 13 - TPCH
		sql = "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_customer.c_custkey, COUNT(c_customer.orderkey) AS c_count FROM (SELECT ca.tpch1.customer.custkey as c_custkey, ca.tpch1.customer.acctbal, ca.tpch1.customer.address, ca.tpch1.customer.comment, ca.tpch1.customer.mktsegment, ca.tpch1.customer.name, ca.tpch1.customer.nationkey, ca.tpch1.customer.phone,ca.tpch1.orders.orderkey, ca.tpch1.orders.clerk, ca.tpch1.orders.comment, ca.tpch1.orders.custkey, ca.tpch1.orders.order_priority, ca.tpch1.orders.orderdate, ca.tpch1.orders.orderstatus, ca.tpch1.orders.ship_priority, ca.tpch1.orders.totalprice FROM ca.tpch1.customer LEFT OUTER JOIN ca.tpch1.orders ON ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.orders.comment NOT LIKE '%special%requests%') c_customer GROUP BY c_customer.c_custkey) c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC";

		//query 19 - TPCH
		sql = SELECT SUM(ca.tpch1.lineitem.extendedprice* (1 - ca.tpch1.lineitem.discount)) AS revenue FROM ca.tpch1.lineitem, ca.tpch1.part WHERE ( ca.tpch1.part.partkey = ca.tpch1.lineitem.partkey AND ca.tpch1.part.brand = 'Brand#12' AND ca.tpch1.part.container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND ca.tpch1.lineitem.quantity >= 1 AND ca.tpch1.lineitem.quantity <= 1 + 10 AND ca.tpch1.part.size BETWEEN 1 AND 5 AND ca.tpch1.lineitem.shipmode IN ('AIR', 'AIR REG') AND ca.tpch1.lineitem.shipinstruct = 'DELIVER IN PERSON' ) OR ( ca.tpch1.part.partkey = ca.tpch1.lineitem.partkey AND ca.tpch1.part.brand = 'Brand#23' AND ca.tpch1.part.container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND ca.tpch1.lineitem.quantity >= 10 AND ca.tpch1.lineitem.quantity <= 10 + 10 AND ca.tpch1.part.size BETWEEN 1 AND 10 AND ca.tpch1.lineitem.shipmode IN ('AIR', 'AIR REG') AND ca.tpch1.lineitem.shipinstruct = 'DELIVER IN PERSON' ) OR ( ca.tpch1.part.partkey = ca.tpch1.lineitem.partkey AND ca.tpch1.part.brand = 'Brand#34' AND ca.tpch1.part.container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND ca.tpch1.lineitem.quantity >= 20 AND ca.tpch1.lineitem.quantity <= 20 + 10 AND ca.tpch1.part.size BETWEEN 1 AND 15 AND ca.tpch1.lineitem.shipmode IN ('AIR', 'AIR REG') AND ca.tpch1.lineitem.shipinstruct = 'DELIVER IN PERSON' )

		//query 20 - TPCH
		sql = SELECT ca.tpch1.supplier.name, ca.tpch1.supplier.address FROM ca.tpch1.supplier, ca.tpch1.nation WHERE ca.tpch1.supplier.suppkey IN (SELECT ca.tpch1.partsupp.suppkey FROM ca.tpch1.partsupp WHERE ca.tpch1.partsupp.partkey IN ( SELECT ca.tpch1.part.partkey FROM ca.tpch1.part WHERE ca.tpch1.part.name LIKE 'forest%') AND ca.tpch1.partsupp.availqty > (SELECT 0.5 * SUM(ca.tpch1.lineitem.quantity) FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.partkey = ca.tpch1.partsupp.partkey AND ca.tpch1.lineitem.suppkey = ca.tpch1.partsupp.suppkey AND ca.tpch1.lineitem.shipdate >= DATE '1994-1-1' AND ca.tpch1.lineitem.shipdate < DATE '1994-1-1' + interval '1' year)) AND ca.tpch1.supplier.nationkey = ca.tpch1.nation.nationkey AND ca.tpch1.nation.name = 'CANADA' ORDER BY ca.tpch1.supplier.name
 * 
 * 
 */