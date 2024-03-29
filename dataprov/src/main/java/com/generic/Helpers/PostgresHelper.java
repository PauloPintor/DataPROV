package com.generic.Helpers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresHelper {
	/**
	 * conn is the private variable where it will be instanced the Postgresql's connection
	 */
	private Connection conn = null;
	private String postgresURL = "";
	private String userName = "";
	private String password = "";
	
	/**
	 * Class constructor where it is initialised the connection with Postgres (conn variable)
	 * 
	 * @throws Exception If variable conn is not initialised or Trino is not initialised
	 */
	public PostgresHelper() throws Exception
	{
		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("Trino is not initialised");
		}
    }

	/**
	 * Class constructor where it is initialised the connection with Postgres (conn variable) with database URL as argument
	 * 
	 * @param databaseURL String with the database URL
	 * @throws Exception if the propertie values are not set
	 */
	public PostgresHelper(String databaseURL) throws Exception{
		postgresURL = databaseURL;

		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("Trino is not initialised");
		}
	}

	/**
	 * Class constructor where it is initialised the connection with Postgres (conn variable) with database URL, user and password as arguments
	 * 
	 * @param databaseURL String with the database URL
	 * @param user String with the user
	 * @param password String with the password
	 * @throws Exception if the propertie values are not set
	 */
	public PostgresHelper(String databaseURL, String userName, String password) throws Exception{
		this.postgresURL = databaseURL;
		this.userName = userName;
		this.password = password;

		if(!this.setConnection()) {
			throw new Exception("The connection variable has not been initialised");
		}
		else if (!this.TestConnection()) {
			throw new Exception("PostgreSQL is not initialised");
		}
	}


	/**
	 * Creates the connection with Postgresql and returns true if the connection is obtained
	 * or false if not.
	 * 
	 * @return boolean
	 * @throws Exception if the propertie values are not set
	 */
	 public boolean setConnection() throws Exception{
		 if (conn != null) {
			 return true;
		 }

		 try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
			   .getConnection("jdbc:postgresql://"+this.postgresURL,
			   this.userName, this.password);
		 } catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		 }

		 return conn != null;
	}

	/**
	 * Close the Postgresql connection
	* 
	* @throws SQLException if not properly instantiated
	*/
	public void closeConnection() throws SQLException{
		if (conn != null)
			conn.close();
    }

	/**
	 * Test if the Postgresql connection is working.
	 * 
	 * @return boolean
	 * @throws Exception if conn is not initialised or if something goes wrong with the test query
	 */
	public boolean TestConnection() throws Exception
	{
		boolean result = false;

		if (conn == null) throw new Exception("The connection variable has not been initialised");

		Statement statement = conn.createStatement();
	    
	    String sql = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";  
	     
	    ResultSet resultSet = statement.executeQuery(sql);

	    if (this.RowCount(resultSet) > 0) result = true;
	    
	    //Clean-up environment
	    resultSet.close();
	    statement.close();
	    
	    return result;
	}

	/**
	 * Return the number of rows in a result obtained in Postgresql.
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
}
