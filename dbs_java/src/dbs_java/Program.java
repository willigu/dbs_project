package dbs_java;

public class Program {
	private static DbBridge dbBridge;
	public static void main(String[] args) {
		
		dbBridge = new DbBridge();
		
		// customize bridge settings
		
		dbBridge.connect();
		
		// Import the stuff from the txt file
		
		dbBridge.close();
	}
}
