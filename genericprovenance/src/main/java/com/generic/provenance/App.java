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
		/*
        String sentence = "(Postgres:Movies:t1.Directors:t4)+((Postgres:Movies:t1.Cassandra:Directors:t4).Postgres:Movies:t3)";
        
        // Regular expression pattern to match words containing ':'
        String regex = "\\w+:[\\w:]+";
        
        // Create a Pattern object
        Pattern pattern = Pattern.compile(regex);
        
        // Create a Matcher object
        Matcher matcher = pattern.matcher(sentence);
        
        // Find and print all matching words
        while (matcher.find()) {
            String word = matcher.group();
            System.out.println(word);
        } 
		*/

        Parser parser = new Parser();
        String a = "SELECT C.region FROM (SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region WHERE A.city = 'Paris' GROUP BY A.region) C INNER JOIN (SELECT m2.region FROM d.public.mypersonnel m2 WHERE position = 'Director')D ON C.region = D.region";

        String b = "SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region GROUP BY A.region";
		
		//"SELECT X.region FROM ("+b+") X INNER JOIN ("+ a +") Y ON X.region = Y.region";
        // "SELECT c.city FROM (SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel UNION SELECT mypersonnel2.city, mypersonnel2.region FROM mypersonnel2 UNION SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel) c";

		//"SELECT d.public.country.country_id FROM d.public.country UNION SELECT c.country_id FROM (SELECT d.public.city.country_id FROM d.public.city inner join d.public.address ON d.public.city.city_id = d.public.address.city_id) c ";
		
		//"SELECT d.public.country.country_id FROM d.public.country UNION SELECT c.country_id FROM (SELECT d.public.city.country_id FROM d.public.city inner join d.public.address ON d.public.city.city_id = d.public.address.city_id) c ";

		//"SELECT job.descr FROM pa.public.person person, pa.public.job job WHERE person.job_id = job.id GROUP BY job.descr";
		
		String sql = "select tpch.public.orders.o_orderpriority, count(*) as order_count from tpch.public.orders where tpch.public.orders.o_orderdate >= date ':1' and exists (select * from tpch.public.lineitem where tpch.public.lineitem.l_orderkey = tpch.public.orders.o_orderkey and tpch.public.lineitem.l_commitdate < tpch.public.lineitem.l_receiptdate)";
		
		
		//"SELECT Movies.title FROM Movies WHERE Movies.rating > 1 AND EXISTS(SELECT title, name FROM DIirectors WHERE Movies.title = Directors.title AND Directors.name = 'Steven Spielberg')";
		
		
		//"SELECT CONCAT(a.first_name,' ',a.last_name) as full_name, f.title, f.description, f.length FROM actor a JOIN film_actor fa ON a.actor_id=fa.actor_id JOIN film f ON f.film_id=fa.film_id";
		
		//"SELECT c.city FROM (SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel UNION SELECT mypersonnel2.city, mypersonnel2.region FROM mypersonnel2 UNION SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel) c";
		
		
		//"SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel UNION ALL SELECT mypersonnel2.city, mypersonnel2.region FROM mypersonnel2";
        
        
        
        //"SELECT C.region FROM (SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region GROUP BY A.region) C INNER JOIN (SELECT m2.region FROM d.public.mypersonnel m2 WHERE position = 'Director')D ON C.region = D.region";  
        
        //"SELECT A.region FROM (SELECT city, region FROM mypersonnel WHERE position like '% agent')A NATURAL JOIN (SELECT city, region FROM mypersonnel	WHERE position = 'Analyst')B GROUP BY A.region";
        
        //"SELECT c.country, ct.city, a.address FROM d.public.country c, d.public.city ct, d.public.address a WHERE c.country_id = ct.country_id AND a.city_id = ct.city_id";
        
        
        //"SELECT c.country, ct.city, a.address FROM d.public.country c inner join d.public.city ct ON c.country_id = ct.country_id INNER JOIN d.public.address a ON a.city_id = ct.city_id WHERE c.country = 'United Kingdom'";
        
        
        //"SELECT c.city, co.country, A.address FROM ( SELECT city_id, city,country_id FROM city ) C NATURAL JOIN ( SELECT country_id, country FROM country ) CO NATURAL JOIN ( SELECT address_id, address, city_id FROM address ) A WHERE co.country = 'United Kingdom'";

        //"SELECT m1.id, m2.name, m1.position FROM p.public.mypersonnel m1 INNER JOIN p.public.mypersonnel m2 ON m1.id = m2.id";

		//query 3 - TPCH
		sql = "SELECT ca.tpch1.lineitem.orderkey, ca.tpch1.orders.orderdate, ca.tpch1.orders.ship_priority, SUM(ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount)) AS revenue FROM ca.tpch1.customer, ca.tpch1.orders, ca.tpch1.lineitem WHERE ca.tpch1.customer.mktsegment = 'BUILDING' AND ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.orders.orderdate < DATE '1995-3-15' AND ca.tpch1.lineitem.shipdate > DATE '1995-3-15' GROUP BY ca.tpch1.lineitem.orderkey, ca.tpch1.orders.orderdate, ca.tpch1.orders.ship_priority ORDER BY revenue DESC, ca.tpch1.orders.orderdate LIMIT 10";

		//query 5 - TPCH
		sql = "SELECT ca.tpch1.nation.name, SUM(ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount)) AS revenue FROM ca.tpch1.customer, ca.tpch1.orders, ca.tpch1.lineitem, ca.tpch1.supplier, ca.tpch1.nation, ca.tpch1.region WHERE ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.lineitem.orderkey = ca.tpch1.orders.orderkey AND ca.tpch1.lineitem.suppkey = ca.tpch1.supplier.suppkey AND ca.tpch1.customer.nationkey = ca.tpch1.supplier.nationkey AND ca.tpch1.supplier.nationkey = ca.tpch1.nation.nationkey AND ca.tpch1.nation.regionkey = ca.tpch1.region.regionkey AND ca.tpch1.region.name = 'ASIA' AND ca.tpch1.orders.orderdate >= DATE '1994-1-1' AND ca.tpch1.orders.orderdate < DATE '1994-1-1' + interval '1' year GROUP BY ca.tpch1.nation.name ORDER BY revenue DESC";

		//query 6 - TPCH
		sql = "SELECT SUM(ca.tpch1.lineitem.extendedprice * ca.tpch1.lineitem.discount) AS revenue FROM ca.tpch1.lineitem WHERE ca.tpch1.lineitem.shipdate >= DATE '1994-1-1' AND ca.tpch1.lineitem.shipdate < DATE '1994-1-1' + interval '1' year AND ca.tpch1.lineitem.discount BETWEEN .06 - 0.01 AND .06 + 0.010001	AND ca.tpch1.lineitem.quantity < 24";

		//query 7 - TPCH
		sql = "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM ( SELECT n1.name AS supp_nation, n2.name AS cust_nation, YEAR(ca.tpch1.lineitem.shipdate) AS l_year, ca.tpch1.lineitem.extendedprice * (1 - ca.tpch1.lineitem.discount) AS volume FROM ca.tpch1.supplier, ca.tpch1.lineitem, ca.tpch1.orders, ca.tpch1.customer, ca.tpch1.nation n1, ca.tpch1.nation n2 WHERE ca.tpch1.supplier.suppkey = ca.tpch1.lineitem.suppkey AND ca.tpch1.orders.orderkey = ca.tpch1.lineitem.orderkey AND ca.tpch1.customer.custkey = ca.tpch1.orders.custkey AND ca.tpch1.supplier.nationkey = n1.nationkey AND ca.tpch1.customer.nationkey = n2.nationkey AND ( (n1.name = 'FRANCE' AND n2.name = 'GERMANY') OR (n1.name = 'GERMANY' AND n2.name = 'FRANCE') ) AND ca.tpch1.lineitem.shipdate BETWEEN DATE '1995-1-1' AND DATE '1996-12-31' ) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year";

        try {
			String newSQL = parser.parseQuery(sql);
            //parser.printWhere();
			//newSQL = "SELECT _job.descr, SUM(_job.age), '(' || listagg('(' ||  _job.prov || ' * ' || cast(_job.age AS varchar)  || ')', '+') WITHIN GROUP (ORDER BY _job.descr) || ')' as prov FROM (SELECT job.descr, person.age, person.prov|| ' x ' || job.prov as prov FROM pa.public.person person, pa.public.job job WHERE person.job_id = job.id) AS _job GROUP BY _job.descr";
			TrinoHelper th = new TrinoHelper();
            ResultSet result = th.ExecuteQuery(newSQL);
			ResultProcess rp = new ResultProcess();
			rp.processing(result);
			rp.printResult();
            //CachedRowSet newResult = th.ResultSetToCachedRowSet(result);
            //th.printQueryResult(newResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
