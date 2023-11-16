# DataPROV
A middleware solution to capture data provenance in centralized and distributed environments, using annotations propagation and query rewriting.

## USAGE	
The solution receives as parameters the database, database URL, the query to parse and a boolean where the users can indicate if they want the why-provenance (wp) result (default is false) using the following command:

    java dataprov.jar -d [DATABASE] -u [DATABASE_URL] -q [QUERY] -wp [TRUE/FALSE - default false]

After the command, it will be asked for the username and password.

### DATA SOURCES
Two testing benchmarks, specifically TPC-H and SSB, were utilized during the experiments for our solution. Our "scripts" folder includes all the necessary scripts for index and table creation. Each table features a "prov" column, and we have provided scripts for generating an easily understandable and simple provenance token for testing purposes.

To generate the data for both benchmarks it is possible to use tools available on their websites.

#### TPC-H

	https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

#### SSB

	https://github.com/Kyligence/ssb-kylin

The queries used in the experimental evaluation are available in the "queries" folder.

## EXAMPLE
Below is an example of a query (TPC-H / Query 4) and the new query with the transformations made by the solution.

```sql
SELECT orders.o_orderpriority, count(*) as order_count 
FROM orders 
WHERE orders.o_orderdate >= date '1993-07-01' 
  AND orders.o_orderdate < date '1993-07-01' + interval '3' month 
  AND EXISTS ( SELECT * 
  			   FROM lineitem 
			   WHERE lineitem.l_orderkey = orders.o_orderkey 
			     AND lineitem.l_commitdate < lineitem.l_receiptdate) 
GROUP BY orders.o_orderpriority 
ORDER BY orders.o_orderpriority

// APPLYING THE RULES TO OBTAIN HOW-PROVENANCE
SELECT orders.o_orderpriority, count(*) AS order_count, STRING_AGG('orders:' || orders.prov|| ' ⊗ ' || nestedT0.prov || ' . ' || CAST(1 as varchar), ' ⊕ ' ORDER BY orders.o_orderpriority) AS prov 
FROM orders JOIN (SELECT orders.o_orderkey, 
				  LISTAGG(C0.prov , ' ⊕ ') WITHIN GROUP ORDER BY orders.o_orderkey AS prov 
				  FROM orders JOIN 
				  	(SELECT lineitem.l_orderkey, 'lineitem:' || lineitem.prov AS prov 
					 FROM lineitem 
					 WHERE lineitem.l_commitdate < lineitem.l_receiptdate
					) AS C0 ON C0.l_orderkey = orders.o_orderkey 
				  GROUP BY orders.o_orderkey
				) AS nestedT0 ON orders.o_orderkey = nestedT0.l_orderkey 
WHERE orders.o_orderdate >= DATE '1993-03-01' AND orders.o_orderdate < DATE '1993-03-01' + INTERVAL '3' month 
GROUP BY orders.o_orderpriority ORDER BY orders.o_orderpriority

```

## ACKNOWLEDGMENTS
This project was carried out as part of a research grant awarded by the Portuguese public agency for science, technology and innovation FCT - Foundation for Science and Technology - under the reference 2021.06773.BD. This work is partially funded by National Funds through the FCT under the Scientific Employment Stimulus - Institutional Call - CEECIN  - ST/00051/2018, and in the context of the projects UIDB/04524/2020 and UIDB/00127/2020.
