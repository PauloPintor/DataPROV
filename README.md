# DataPROV
A middleware solution to capture data provenance in centralized and distributed environments, using annotations propagation and query rewriting.

## USAGE	
The solution receives as parameters the database, database URL, the query to parse and a boolean where the users can indicate if they want the why-provenance (wp) result (default is false) using the following command:

    java dataprov.jar -d [DATABASE] -u [DATABASE_URL] -q [QUERY] -wp [TRUE/FALSE - default false]

After the command, it will be asked for the username and password.

### DATA SOURCES

