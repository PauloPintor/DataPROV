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
        String sql = "SELECT c.country, ct.city, a.address FROM d.public.country c, d.public.city ct, d.public.address a WHERE c.country_id = ct.country_id AND a.city_id = ct.city_id";
        
        
        
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
