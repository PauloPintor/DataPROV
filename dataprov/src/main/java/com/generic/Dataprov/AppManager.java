package com.generic.Dataprov;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.generic.Helpers.PostgresHelper;
import com.generic.Helpers.TrinoHelper;
import com.generic.Parser.Parser;
import com.generic.ResultProcess.ResultProcess;
import com.ibm.icu.util.LocaleMatcher.Result;

public class AppManager {
    private String database;
    private String databaseURL;
	private String query;
	private boolean withWhy = false;
    private String user;
    private String password;
    private boolean ssl = false;

    public AppManager(String databse, String databaseURL, String query, boolean withWhy) throws Exception {
        this.database = databse;
        this.databaseURL = databaseURL;
        this.query = query;
        this.withWhy = withWhy;

        if(database.toLowerCase().compareTo("postgres") == 0)
			execPostgresl();
		else if(database.toLowerCase().compareTo("trino") == 0)
		    exeTrino();
    }

    public void execPostgresl() throws Exception{
        getUserPassword();

		PostgresHelper ph = new PostgresHelper(databaseURL, user, password);
		ResultSet result = ph.ExecuteQuery(parseQuery());
		printResult(result);

    }

    public void exeTrino() throws Exception{
        getUserPassword();

		TrinoHelper ph = new TrinoHelper(databaseURL, user, password, ssl);
		ResultSet result = ph.ExecuteQuery(parseQuery());
		printResult(result);
    }

    private String parseQuery() throws Exception{
        Parser parser = new Parser(database);
		return parser.rewriteQuery(query);
    }

    private void printResult(ResultSet result) throws SQLException{
        ResultProcess rp = new ResultProcess();
        if(withWhy) {				
		    rp.processing(result);
		}
        rp.printResult();
    }

    private void getUserPassword(){
        System.out.println("Enter the user name: ");
        this.user = System.console().readLine();
        System.out.println("Enter the password: ");
        this.password = System.console().readLine();
        System.out.println("SSL connection (True/False - default false): ");
		this.ssl = Boolean.parseBoolean(System.console().readLine());
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabaseURL() {
        return this.databaseURL;
    }

    public void setDatabaseURL(String databaseURL) {
        this.databaseURL = databaseURL;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isWithProv() {
        return this.withWhy;
    }

    public boolean getWithWhy() {
        return this.withWhy;
    }

    public void setWithWhy(boolean withWhy) {
        this.withWhy = withWhy;
    }
}
