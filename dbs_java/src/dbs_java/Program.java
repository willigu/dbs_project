package dbs_java;

import java.sql.SQLException;

public class Program {
	public static void main(String[] args) {
		
		DbBridge dbBridge = new DbBridge();
		
		// customize bridge settings
		
		dbBridge.connect();
		
		// Import the stuff from the txt file
		try {
			insert_data id = new insert_data(dbBridge);
			id.import_data();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		dbBridge.close();
	}
}
