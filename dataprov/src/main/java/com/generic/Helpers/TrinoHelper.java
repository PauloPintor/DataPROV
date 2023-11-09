package com.generic.Helpers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

/** 
 * This class is responsible to connect with Trino and execute queries
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class TrinoHelper {

	/**
	 * conn is the private variable where it will be instanced the Trino's connection
	 */
	private Connection conn = null;
	private String trinoURL = "";
	private String userName = "";
	private String password = "";
	private boolean ssl = false;
	
	/**
	 * Class constructor where it is initialised the connection with Trino (conn variable)
	 * 
	 * @throws Exception If variable conn is not initialised or Trino is not initialised
	 */
	public TrinoHelper() throws Exception
	{
		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("Trino is not initialised");
		}
    }
	
	/**
	 * Class constructor where it is initialised the connection with Trino (conn variable) with database URL as argument
	 * 
	 * @param databaseURL String with the database URL
	 * @throws Exception if the propertie values are not set
	 */
	public TrinoHelper(String databaseURL) throws Exception
	{
		trinoURL = databaseURL;
		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("Trino is not initialised");
		}
	}

	/**
	 * Class constructor where it is initialised the connection with Postgres (conn variable) with database URL, user, password and SSL as arguments
	 * 
	 * @param databaseURL String with the database URL
	 * @param user String with the user
	 * @param password String with the password
	 * @param ssl boolean with the ssl option
	 * @throws Exception if the propertie values are not set
	 */
	public TrinoHelper(String databaseURL, String userName, String password, boolean ssl) throws Exception
	{
		this.trinoURL = databaseURL;
		this.userName = userName;
		this.password = password;
		this.ssl = ssl;

		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("Trino is not initialised");
		}
	}

	/**
	 * Creates the connection with Trino and returns true if the connection is obtained
	 * or false if not.
	 * 
	 * @return boolean
	 * @throws Exception if the propertie values are not set
	 */
	 public boolean setConnection() throws Exception{
		 if (conn != null) {
			 return true;
		 }
		 
		 String url = "jdbc:trino://"+trinoURL;
		 
	     Properties properties = new Properties();
		 properties.setProperty("user", this.userName);
		 properties.setProperty("password", this.password);
		 properties.setProperty("SSL", ssl ? "true" : "false");
		 
		 conn = DriverManager.getConnection(url, properties);

		 return conn != null;
	}
    
	 /**
	  * Close the Trino connection
	  * 
	  * @throws SQLException if not properly instantiated
	  */
	public void closeConnection() throws SQLException{
		if (conn != null)
			conn.close();
    }
 
	/**
	 * Test if the Trino connection is working.
	 * 
	 * @return boolean
	 * @throws Exception if conn is not initialised or if something goes wrong with the test query
	 */
	public boolean TestConnection() throws Exception
	{
		boolean result = false;

		if (conn == null) throw new Exception("The connection variable has not been initialised");

		Statement statement = conn.createStatement();
	    
	    String sql = "SHOW CATALOGS";  
	     
	    ResultSet resultSet = statement.executeQuery(sql);

	    if (this.RowCount(resultSet) > 0) result = true;
	    
	    //Clean-up environment
	    resultSet.close();
	    statement.close();
	    
		return result;
	}
	
	/**
	 * Return the number of rows in a result obtained in Trino.
	 * 
	 * @param result the input is a ResultSet variable
	 * @return integer
	 * @throws SQLException if Resultset is not properly instantiated
	 */
	public int RowCount(ResultSet result) throws SQLException {
		int i = 0;
        
		while (result.next()) {
        	i++;
        }
        
        return i;
	}

	/**
	 * Executes a query in Trino and returns the ResltSet of the result
	 *
	 * @param query the string with the query expression
	 * @return ResultSet
	 * @throws Exception if conn is null or something goes wrong with the query
	 */
	public ResultSet ExecuteQuery(String query) throws Exception {
		if (conn == null) throw new Exception("The connection variable has not been initialised");

		Statement statement = conn.createStatement();
	
		ResultSet result = statement.executeQuery(query);

		return result;
	}

	/**
	 * Prints a query result based on the ResultSet in order to be readable
	 *
	 * @param result a ResultSet containing the result of a query
	 * @throws SQLException if the result has sql problems
	 */
	public void printQueryResult(ResultSet result) throws SQLException {

		ResultSetMetaData rsmd = result.getMetaData();
		int columnsNumber = rsmd.getColumnCount();

		for (int i = 1; i <= columnsNumber; i++) {
			if (i > 1) System.out.print(" | ");
			System.out.print(rsmd.getColumnName(i) + " (" + rsmd.getColumnTypeName(i) + ") ");
		}
		System.out.println(" ");

		while (result.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				if (i > 1) System.out.print(" | ");
				System.out.print(result.getString(i));
			}
			System.out.println(" ");
		}

		result.close();
	}

	
	/** 
	 * 
	 * The function ResultSetToCachedRowSet converts a ResultSet to a CachedRowSet
	 * 
	 * @param result
	 * @return CachedRowSet
	 * @throws SQLException
	 */
	public CachedRowSet ResultSetToCachedRowSet(ResultSet result) throws SQLException{
		CachedRowSet crs = RowSetProvider.newFactory().createCachedRowSet();

		crs.populate(result);
		
		return crs;
	}
}
