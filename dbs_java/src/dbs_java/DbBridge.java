package dbs_java;

import java.sql.*;

public class DbBridge {
	
	public DbBridge(){
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
