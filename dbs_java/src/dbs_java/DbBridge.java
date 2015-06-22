package dbs_java;

import java.sql.*;
import java.util.Properties;

/**
 * A Postgresql db wrapper class
 * @author william
 *
 */
public class DbBridge {
	private String strServer, strPort, strDBName;
	private String strUsername, strPwd;
	Connection dbConnection;
	/**
	 * Create a new bridge instance with default values
	 */
	public DbBridge(Properties properties){
		try {
			
			// load DB driver from external JAR
			Class.forName("org.postgresql.Driver");
			
			this.strServer = properties.getProperty("server");
			this.strPort = properties.getProperty("port");
			this.strUsername = properties.getProperty("username");
			this.strPwd = properties.getProperty("pwd");
			this.strDBName = properties.getProperty("dbname");
			
		} catch (ClassNotFoundException e) {
			System.out.println("Error: Database driver could not be loaded!");
		}
	}
	
	public String getPort(){
		return this.strPort;
	}
	
	public String getServer(){
		return this.strServer;
	}
	
	/**
	 * Connects to the database 
	 * @return true, if connecting process was successfull
	 */
	public boolean connect(){
		String url = "jdbc:postgresql://" + this.strServer + ":" + this.strPort + "/" + this.strDBName;
		try {
			this.dbConnection =  DriverManager.getConnection(url,strUsername,strPwd);
		} catch (SQLException e) {
			System.out.println("Error: could not connect to the database using the given data.");
			return false;
		}
		return true;
	}
	
	/**
	 * Executes a given query string
	 * @param strQuery
	 */
	public ResultSet x(String strQuery){
		ResultSet res;
		try {
			Statement query = dbConnection.createStatement();
			res = query.executeQuery(strQuery);
			query.close();
		} catch (SQLException e) {
			System.out.println("Error during query execution!");
			return null;
		}
		return res;
	}
	
	/**
	 * Disconnecting from database
	 * @return true, if closing the connection was succesfull
	 */
	public boolean close(){
		try {
			this.dbConnection.close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Error: connection could not be closed!");
			return false;
		}
	}

}
