# DataPROV
A middleware solution to capture data provenance in centralized and distributed environments, using annotations propagation and query rewriting.

## USAGE	
The solution receives as parameters the database, database URL, the query to parse and a boolean where the users can indicate if they want the why-provenance (wp) result (default is false) using the following command:

    java dataprov.jar -d [DATABASE] -u [DATABASE_URL] -q [QUERY] -wp [TRUE/FALSE - default false]

After the command, it will be asked for the username and password.

### DATA SOURCES
Two benchmarks were used for the experimental tests of our solution: TPC-H and SSB. The scripts for creating the tables and indexes are available in the "scripts" folder.

To generate the data for both benchmarks it is possible to use tools available on their websites.

#### TPC-H

	https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

#### SSB

	https://github.com/Kyligence/ssb-kylin

The queries used in the experimental evaluation are available in the "queries" folder.
