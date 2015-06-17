package dbs_java;

import java.sql.*;
import java.util.Properties;

/**
 * A Postgresql db wrapper class
 * @author william
 *
 */
public class DbBridge {
	String strServer, strPort, strDBName, strURL;
	String strUsername, strPwd;
	Connection dbConnection;
	String strLastQuery;
	/**
	 * Create a new bridge instance with default values
	 */
	public DbBridge(){
		try {
			
			// load DB driver fro external JAR
			Class.forName("org.postgresql.Driver");
			this.strServer = "localhost";
			this.strPort = "15432";
			this.strDBName = "dbs_project";
		} catch (ClassNotFoundException e) {
			System.out.println("Error: Database driver could not be loaded!");
		}
	}
	
	public void setPort(String port){
		this.strPort = port;
	}
	
	public String getPort(){
		return this.strPort;
	}
	
	public String getServer(){
		return this.strServer;
	}
	
	public void setServer(String server){
		this.strServer = server;
	}
	
	public void setUsername(String name){
		this.strUsername = name;
	}
	
	public void setPassword(String pwd){
		this.strPwd = pwd;
	}
	
	public void setDbName(String name){
		this.strDBName = name;
	}
	
	/**
	 * Connects to the database 
	 * @return true, if connecting process was successfull
	 */
	public boolean connect(){
		String url = "jdbc:postgresql://" + this.strServer + ":" + this.strPort + "/" + this.strDBName;
		Properties props = new Properties();
		props.setProperty("user",this.strUsername);
		props.setProperty("password",this.strPwd);
		try {
			this.dbConnection =  DriverManager.getConnection(url, props);
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
