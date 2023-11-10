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

```sql
select orders.o_orderpriority, count(*) as order_count from orders where orders.o_orderdate >= date '1993-07-01' and orders.o_orderdate < date '1993-07-01' + interval '3' month and exists ( select * from lineitem where lineitem.l_orderkey = orders.o_orderkey and lineitem.l_commitdate < lineitem.l_receiptdate ) group by orders.o_orderpriority order by orders.o_orderpriority

// APPLYING THE RULES 

sql`SELECT 1`
// passes
```