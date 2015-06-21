package dbs_java;

public class Program {
	public static void main(String[] args) {
		
		DbBridge dbBridge = new DbBridge();
		
		// customize bridge settings
		
		dbBridge.connect();
		
		// Import the stuff from the txt file
		insert_data id = new insert_data(dbBridge);
		id.import_data();
		
		dbBridge.close();
	}
}
