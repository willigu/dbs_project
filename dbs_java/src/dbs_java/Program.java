package dbs_java;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class Program {
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("./data/config.conf"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		// to create a config.conf file uncomment the following code (DONT PUSH THE PASSWORD!)
		/*
		properties.setProperty("server", "comm.epow0.org");
		properties.setProperty("port", "5432");
		properties.setProperty("username", "dbprojekt");
		properties.setProperty("pwd", "PASSWORT"); // insert password here but dont push it to github
		properties.setProperty("dbname", "dbprojekt");
		
		try {
			properties.store(new FileOutputStream("./data/config.conf"), "This file is autogenerated");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		DbBridge dbBridge = new DbBridge(properties);
		
		// customize bridge settings
		
		dbBridge.connect();
		
		// Import our data
		try {
			insert_data id = new insert_data(dbBridge);
			id.import_data("./data/imdb_top100t_2015-06-18.csv");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		dbBridge.close();
	}
}
