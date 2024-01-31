package com.generic.Dataprov;

import java.io.Console;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.generic.Helpers.PostgresHelper;
import com.generic.Helpers.TrinoHelper;
import com.generic.Parser.Parser;
import com.generic.ResultProcess.ResultProcess;

public class AppManager {
    private String database;
    private String databaseURL;
	private String query;
	private boolean withWhy = false;
	private boolean withTime = false;
	private boolean withTrasnform = false;
    private String user = "";
    private String password = "";
    private boolean ssl = false;

    public AppManager(String database, String databaseURL, String query, boolean withWhy, boolean withTrasnform, boolean withTime) throws Exception {
        this.database = database;
        this.databaseURL = databaseURL;
        this.query = query;
        this.withWhy = withWhy;
		this.withTime = withTime;
		this.withTrasnform = withTrasnform;

        if(database.toLowerCase().compareTo("postgres") == 0)
			execPostgresl();
		else if(database.toLowerCase().compareTo("trino") == 0)
		    exeTrino();
    }

	public AppManager(String databaseURL, String query, boolean withTime) throws Exception {
        this.databaseURL = databaseURL;
        this.query = query;
		this.withTime = withTime;
	}

    public void execPostgresl() throws Exception{
		if(user.compareTo("") == 0 || password.compareTo("") == 0)
        	getUserPassword();		
		
		long startTime = 0;
		long endTime = 0;

		if (withTime) {
			startTime = System.currentTimeMillis();
		}
		//PostgresHelper ph = new PostgresHelper(databaseURL, user, password);
		PostgresHelper ph = new PostgresHelper(databaseURL, user, "1904ieetaPPtese");
		ResultSet result = ph.ExecuteQuery(parseQuery());
		if (withTime) {
			endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0; 
			System.out.println("Time of query execution: "+time + "seconds");
		}
			
		printResult(result);

    }

	public void execPostgresl(String query) throws Exception{
		if(user.compareTo("") == 0 || password.compareTo("") == 0)
			getUserPassword();

		long startTime = 0;
		long endTime = 0;

		if (withTime) {
			startTime = System.currentTimeMillis();
		}
		PostgresHelper ph = new PostgresHelper(databaseURL, user, password);
		ResultSet result = ph.ExecuteQuery(query);
		ph.closeConnection();

		if (withTime) {
			endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0; 
			System.out.println("Time of query execution: "+time + "seconds");
		}
		
		printResult(result);

    }

    public void exeTrino() throws Exception{
        getUserPassword();

		TrinoHelper ph = new TrinoHelper(databaseURL, user, password, ssl);
		ResultSet result = ph.ExecuteQuery(parseQuery());
		printResult(result);
    }

    public String parseQuery() throws Exception{
        Parser parser = new Parser(database);
		long startTime = 0;
		long endTime = 0;

		if (withTime) {
			startTime = System.currentTimeMillis();
		}
		String queryParsed = parser.rewriteQuery(query);

		if (withTime) {
			endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0; 
			if(withTrasnform){
				System.out.println();
				System.out.println("**************************QUERY TRANSFORMED**************************");
				System.out.println();
				System.out.println("Query transformed: " + queryParsed);
			}
			System.out.println();
			System.out.println("**************************TIMES**************************");
			System.out.println();

			System.out.println("Time of parsing query: " + time + "seconds");
		}else if(withTrasnform){
			System.out.println();
			System.out.println("**************************QUERY TRANSFORMED**************************");
			System.out.println();
			System.out.println("Query transformed: " + queryParsed);
		}

		return queryParsed;
    }

	private void generateWhy(ResultSet result) throws SQLException
	{
		long startTime = 0;
		long endTime = 0;

		if (withTime) {
			startTime = System.currentTimeMillis();
		}
		ResultProcess rp = new ResultProcess(withWhy);
		rp.processing(result);
		if (withTime) {
			endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0; 
			System.out.println("Time of generate why: " + time + "seconds");
		}
	}	

    private void printResult(ResultSet result) throws SQLException{
		long startTime = 0;
		long endTime = 0;

		if (withTime && withWhy) {
			startTime = System.currentTimeMillis();
		}
        ResultProcess rp = new ResultProcess(withWhy);
		
		rp.processing(result);
		if (withTime && withWhy) {
			endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0; 
			System.out.println("Time of generate why: "+time + "seconds");
		}
        rp.printResult();
    }

    private void getUserPassword(){
        System.out.println("Enter the user name: ");
        this.user = System.console().readLine();
        System.out.println("Enter the password: ");
		char[] passwordChars = System.console().readPassword();
        this.password = new String(passwordChars);
        System.out.println("SSL connection (True/False - default false): ");
		this.ssl = Boolean.parseBoolean(System.console().readLine());
    }

	public void setUserName(String user){ this.user = user; }

	public void setPassword(String password){ this.password = password; }

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
