package com.generic.provenance;

import java.sql.ResultSet;

import javax.sql.rowset.CachedRowSet;

import com.generic.Helpers.TrinoHelper;
import com.generic.Parser.Parser;

public class App 
{
    public static void main( String[] args )
    {
        Parser parser = new Parser();
        String a = "SELECT C.region FROM (SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region WHERE A.city = 'Paris' GROUP BY A.region) C INNER JOIN (SELECT m2.region FROM d.public.mypersonnel m2 WHERE position = 'Director')D ON C.region = D.region";

        String b = "SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region GROUP BY A.region";
		
		//"SELECT X.region FROM ("+b+") X INNER JOIN ("+ a +") Y ON X.region = Y.region";
        // "SELECT c.city FROM (SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel UNION SELECT mypersonnel2.city, mypersonnel2.region FROM mypersonnel2 UNION SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel) c";
		
		String sql = "SELECT d.public.country.country_id FROM d.public.country UNION SELECT c.country_id FROM (SELECT d.public.city.country_id FROM d.public.city inner join d.public.address ON d.public.city.city_id = d.public.address.city_id) c ";
		
		
		//"SELECT mypersonnel.city, mypersonnel.region FROM mypersonnel UNION ALL SELECT mypersonnel2.city, mypersonnel2.region FROM mypersonnel2";
        
        
        
        //"SELECT C.region FROM (SELECT A.region FROM ( SELECT m1.city, m1.region FROM d.public.mypersonnel m1 WHERE position like '% agent')A INNER JOIN (SELECT m1.city, m1.region FROM d.public.mypersonnel2 m1 WHERE position = 'Analyst')B ON A.city = B.city AND A.region = B.region GROUP BY A.region) C INNER JOIN (SELECT m2.region FROM d.public.mypersonnel m2 WHERE position = 'Director')D ON C.region = D.region";  
        
        //"SELECT A.region FROM (SELECT city, region FROM mypersonnel WHERE position like '% agent')A NATURAL JOIN (SELECT city, region FROM mypersonnel	WHERE position = 'Analyst')B GROUP BY A.region";
        
        //"SELECT c.country, ct.city, a.address FROM d.public.country c, d.public.city ct, d.public.address a WHERE c.country_id = ct.country_id AND a.city_id = ct.city_id";
        
        
        //"SELECT c.country, ct.city, a.address FROM d.public.country c inner join d.public.city ct ON c.country_id = ct.country_id INNER JOIN d.public.address a ON a.city_id = ct.city_id WHERE c.country = 'United Kingdom'";
        
        
        //"SELECT c.city, co.country, A.address FROM ( SELECT city_id, city,country_id FROM city ) C NATURAL JOIN ( SELECT country_id, country FROM country ) CO NATURAL JOIN ( SELECT address_id, address, city_id FROM address ) A WHERE co.country = 'United Kingdom'";

        //"SELECT m1.id, m2.name, m1.position FROM p.public.mypersonnel m1 INNER JOIN p.public.mypersonnel m2 ON m1.id = m2.id";
        try {
            String newSQL = parser.parseQuery(sql);

            TrinoHelper th = new TrinoHelper();
            ResultSet result = th.ExecuteQuery(newSQL);
            CachedRowSet newResult = th.ResultSetToCachedRowSet(result);
            th.printQueryResult(newResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
