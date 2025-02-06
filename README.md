# DataPROV
A middleware solution to capture data provenance in centralized and distributed environments, using annotations propagation and query rewriting.

## USAGE	
The solution receives as parameters the database, database URL, and the query to parse. It also has optional parameters: -wb If true, the boolean provenance (B[X]) is computed; -wt If true, the trio provenance (Trio(X)) is computed; -wp If true, the why-provenance is computed; -wpos If true, the positive boolean provenance (PosBool(X)) is computed; -wl If true, the lineage provenance (Lin(X)) is computed; -t If true, the execution time is computed; -nq If true, the transformed query is printed; -np If true, the provenance is not computed; -dbi If true, the provenance tokens will contain database information (database:schema:table:token); -or If true, the result has the original result, in case of false it shows the result with 1k or 0k. An example of the command is the following:


    java dataprov.jar -d [DATABASE] -u [DATABASE_URL] -q [QUERY] -wb/wt/wp/wpos/wlin [TRUE/FALSE - default false] -t [TRUE/FALSE - default false] -nq [TRUE/FALSE - default false] -np [TRUE/FALSE - default false] -dbi [TRUE/FALSE - default false] -or [TRUE/FALSE - default false]

After the command, it will be asked for the username and password.

### DATA SOURCES
Three testing benchmarks, specifically TPC-H, SSB and JackPine, were utilized during the experiments for our solution. Our "scripts" folder includes all the necessary scripts for index and table creation. Each table features a "prov" column, and we have provided scripts for generating an easily understandable and simple provenance token for testing purposes.

To generate the data for both benchmarks it is possible to use tools available on their websites.

#### TPC-H

	https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

#### SSB

	https://github.com/Kyligence/ssb-kylin

#### JackPine

	https://github.com/debjyoti385/jackpine

The queries used in the experimental evaluation are available in the "queries" folder.

## EXAMPLE
Below is an example of a query (TPC-H / Query 4) and the new query with the transformations made by the solution.

```sql
select supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment 
from part, supplier, partsupp, nation, region 
where part.p_partkey = partsupp.ps_partkey 
  and supplier.s_suppkey = partsupp.ps_suppkey 
  and part.p_size = 33 
  and part.p_type like '%BRASS' 
  and supplier.s_nationkey = nation.n_nationkey 
  and nation.n_regionkey = region.r_regionkey 
  and region.r_name = 'ASIA' 
  and partsupp.ps_supplycost = (select min(partsupp.ps_supplycost) 
  								from partsupp, supplier, nation, region 
								where part.p_partkey = partsupp.ps_partkey 
								and supplier.s_suppkey = partsupp.ps_suppkey 
								and supplier.s_nationkey = nation.n_nationkey 
								and nation.n_regionkey = region.r_regionkey 
								and region.r_name = 'ASIA') 
order by supplier.s_acctbal desc, nation.n_name, supplier.s_name, part.p_partkey 
LIMIT 100

// APPLYING THE RULES TO OBTAIN PROVENANCE POLYNOMIALS
SELECT supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment, part.prov || ' . ' || partsupp.prov || ' . ' || supplier.prov || ' . ' || nation.prov || ' . ' || region.prov || ' . ' || '(' || C0.prov || ')'||'. [' || C0.F0|| '= 1 ⊗' || partsupp.ps_supplycost|| ']' AS prov 
FROM part INNER JOIN partsupp ON part.p_partkey = partsupp.ps_partkey INNER JOIN supplier ON supplier.s_suppkey = partsupp.ps_suppkey INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey INNER JOIN region ON nation.n_regionkey = region.r_regionkey INNER JOIN (SELECT partsupp.ps_partkey, STRING_AGG(CONCAT('(',partsupp.prov || ' . ' || supplier.prov || ' . ' || nation.prov || ' . ' || region.prov, ')', ' ⊗ ', partsupp.ps_supplycost), ' +min ') AS F0, CONCAT('δ(',STRING_AGG(partsupp.prov || ' . ' || supplier.prov || ' . ' || nation.prov || ' . ' || region.prov, ' + '),')') AS prov 
						FROM partsupp INNER JOIN supplier ON supplier.s_suppkey = partsupp.ps_suppkey INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey INNER JOIN region ON nation.n_regionkey = region.r_regionkey 
						WHERE region.r_name = 'ASIA' GROUP BY partsupp.ps_partkey) AS C0 ON part.p_partkey = C0.ps_partkey 
WHERE part.p_size = 33 
	AND part.p_type LIKE '%BRASS' 
	AND region.r_name = 'ASIA' 
ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey 
LIMIT 100

```

## ACKNOWLEDGMENTS
This project was carried out as part of a research grant awarded by the Portuguese public agency for science, technology and innovation FCT - Foundation for Science and Technology - under the reference 2021.06773.BD. This work is partially funded by National Funds through the FCT under the Scientific Employment Stimulus - Institutional Call - CEECIN  - ST/00051/2018, and in the context of the projects UIDB/04524/2020 and UIDB/00127/2020.
