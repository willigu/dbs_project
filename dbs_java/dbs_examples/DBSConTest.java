/** 
 * 
 * License: MIT License (see LICENCE file)
 * Copyright (c) 2015 Daniel Kressner
 * 
 * @author Daniel Kressner
 *
 */

package de.fu_berlin.mi.ag_db.dbs.tutjdbc;

import java.sql.*;
import java.util.Properties;

public class DBSConTest {

	public static void main(String[] args) {
		try {
			/* Load Driver (included in jar file from JDBC database provider */
			Class.forName("org.postgresql.Driver");

			/* Connect String and Credentials for DB connection*/
			String server = "localhost";
			String port = "15432";
			String dbName = "myapp";
			String url = "jdbc:postgresql://" + server + ":" + port + "/" + dbName;
			Properties props = new Properties();
			props.setProperty("user","myapp");
			props.setProperty("password","dbpass");
			Connection conn = DriverManager.getConnection(url, props);

			/* Alternative Connect String without Properties */
			// String url = "jdbc:postgresql://localhost:15432/myapp?user=myapp&password=dbpass";
			// Connection conn = DriverManager.getConnection(url);

			/* Simple Query 1: Access Attribute by Position */
			Statement st1 = conn.createStatement();
			ResultSet rs1 = st1.executeQuery("SELECT * FROM weather");
			while (rs1.next()) {
				System.out.print("Column 1 returned ");
				System.out.println(rs1.getString(1));
			}
			rs1.close();
			st1.close();

			/* Simple Query 2: Access Attribute by Name */
			Statement st2 = conn.createStatement();
			ResultSet rs2 = st2.executeQuery("SELECT * FROM weather");
			while (rs2.next()) {
				String city = rs2.getString("city");
				int tempLow = rs2.getInt("temp_lo");
				int tempHigh = rs2.getInt("temp_hi");
				double prcp = rs2.getDouble("prcp");
				Date date = rs2.getDate("date");
				System.out.println(city + "\t" + tempLow + "\t" + tempHigh
						+ "\t" + prcp + "\t" + date);
			}
			rs2.close();
			st2.close();

			/* Simple Query 3: With Parameter BAD!!! */
			String myCity = "\'Berkeley\'";
			Statement st3 = conn.createStatement();
			ResultSet rs3 = st3
					.executeQuery("SELECT * FROM weather WHERE city like "
							+ myCity);
			while (rs3.next()) {
				String city = rs3.getString("city");
				int tempLow = rs3.getInt("temp_lo");
				int tempHigh = rs3.getInt("temp_hi");
				double prcp = rs3.getDouble("prcp");
				Date date = rs3.getDate("date");
				System.out.println(city + "\t" + tempLow + "\t" + tempHigh
						+ "\t" + prcp + "\t" + date);
			}
			rs3.close();
			st3.close();

			/* Simple Query 4: With Parameter BAD!!! SQL Injection */
			myCity = "\'Berkeley\' OR true";
			Statement st4 = conn.createStatement();
			ResultSet rs4 = st4
					.executeQuery("SELECT * FROM weather WHERE city like "
							+ myCity);
			while (rs4.next()) {
				String city = rs4.getString("city");
				int tempLow = rs4.getInt("temp_lo");
				int tempHigh = rs4.getInt("temp_hi");
				double prcp = rs4.getDouble("prcp");
				Date date = rs4.getDate("date");
				System.out.println(city + "\t" + tempLow + "\t" + tempHigh
						+ "\t" + prcp + "\t" + date);
			}
			rs4.close();
			st4.close();

			/* Solution: Prepared Statement */
			int tempHigh = 50;
			String cityName = "San Francisco";
			PreparedStatement pst = conn
			// .prepareStatement("SELECT * FROM weather WHERE temp_hi > ?");
					.prepareStatement("SELECT * FROM weather WHERE city like ?");
			// pst.setInt(1, tempHigh);
			pst.setString(1, cityName);
			ResultSet rsp = pst.executeQuery();
			while (rsp.next()) {
				System.out.print("Column 1 returned ");
				System.out.println(rsp.getString(1));
			}
			rsp.close();
			pst.close();

			/* Cursor */
			// make sure autocommit is off
			conn.setAutoCommit(false);
			Statement stc = conn.createStatement();

			// Turn use of the cursor on.
			stc.setFetchSize(50);
			ResultSet rsc = stc.executeQuery("SELECT * FROM weather");
			while (rsc.next()) {
				System.out.println("a row was returned.");
			}
			rsc.close();

			// Turn the cursor off.
			stc.setFetchSize(0);
			rsc = stc.executeQuery("SELECT * FROM weather");
			while (rsc.next()) {
				System.out.println("many rows were returned.");
			}
			rsc.close();
			stc.close();

			/* Insert */
			String city = "San Francisco";
			int tempLow = 50;
			int tempHi = 58;
			double prcp = 0.3;
			// java.sql.Date depricated!
			Date curTime = new Date(System.currentTimeMillis());

			PreparedStatement inst = conn
					.prepareStatement("INSERT INTO weather VALUES (?, ?, ?, ?, ?)");
			inst.setString(1, city);
			inst.setInt(2, tempLow);
			inst.setInt(3, tempHi);
			inst.setDouble(4, prcp);
			inst.setDate(5, curTime);
			int rowsInsert = inst.executeUpdate();
			System.out.println(rowsInsert + " rows inserted");
			inst.close();

			/* Delete */
			PreparedStatement std = conn
					.prepareStatement("DELETE FROM weather WHERE date = ?");
			std.setDate(1, curTime);
			int rowsDeleted = std.executeUpdate();
			System.out.println(rowsDeleted + " rows deleted");
			std.close();

			/* Always Close the Connection */
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}