package dbs_java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * A Postgresql db wrapper class
 * 
 * @author william
 *
 */
public class DbBridge
{
   private String strServer, strPort, strDBName;
   private String strUsername, strPwd;
   private Connection dbConnection;
   /**
    * Do INSERT without any additional options
    */
   public static final int INSERT_NO_OPTION = 0;

   /**
    * Do INSERT if row is not already in the table
    */
   public static final int INSERT_IF_NON_EXISTENT = 1;

   /**
    * Adds a RETURNING clause to an INSERT query
    */
   public static final int INSERT_RETURNING_ID = 2;

   /**
    * Indicates a DEFAULT value in a query
    */
   public static final String DEFAULT_VALUE = "DEFAULT_VAL";

   /**
    * Indicated a NULL value
    */
   public static final String NULL_VALUE = "NULL_VAL";

   /**
    * Create a new bridge instance with default values
    */
   public DbBridge(Properties properties)
   {
      try
      {

         // load DB driver from external JAR
         Class.forName("org.postgresql.Driver");

         this.strServer = properties.getProperty("server");
         this.strPort = properties.getProperty("port");
         this.strUsername = properties.getProperty("username");
         this.strPwd = properties.getProperty("pwd");
         this.strDBName = properties.getProperty("dbname");

      }
      catch (ClassNotFoundException e)
      {
         System.out.println("Error: Database driver could not be loaded!");
      }
   }

   /**
    * Connects to the database
    * 
    * @return true, if connecting process was successfull
    */
   public boolean connect()
   {
      String url = "jdbc:postgresql://" + this.strServer + ":" + this.strPort + "/" + this.strDBName;
      try
      {
         this.dbConnection = DriverManager.getConnection(url, strUsername, strPwd);
      }
      catch (SQLException e)
      {
         e.printStackTrace();
         System.out.println("Error: could not connect to the database using the given data.");
         return false;
      }
      return true;
   }

   /**
    * Executes a given query string
    * 
    * @param strQuery
    */
   public ResultSet x(String strQuery)
   {
      ResultSet res;
      try
      {
         Statement query = dbConnection.createStatement();
         res = query.executeQuery(strQuery);
      }
      catch (SQLException e)
      {
         System.out.println("Error during query execution!");
         return null;
      }
      return res;
   }

   /**
    * Executes an INSERT query
    * 
    * @param strTable
    *           name of the table
    * @param insertMap
    *           mapping of columnName and value
    * @param colDataTypes
    *           specify the data types of the values in the mapping
    * @param insertFlags
    *           customize INSERT query with flag options
    * @return a SQL result set
    */
   public ResultSet insert(String strTable, LinkedHashMap<String, String> insertMap, String[] colDataTypes,
         final int insertFlags)
   {
      ResultSet res = null;
      String strQuery = "INSERT INTO " + strTable + " ";
      String strValues = "";
      String strIdCol = null;
      try
      {
         // build INSERT query
         strQuery += "(";
         for (String key : insertMap.keySet())
         {
            strQuery += key + " ,";

            // handle DEFAULT cases
            if (!insertMap.get(key).equals(DEFAULT_VALUE))
               strValues += "?,";
            else
               strValues += "nextval('" + strTable.toLowerCase() + "_" + key.toLowerCase() + "_seq'),";

            // set first Id field available
            if (key.contains("Id") && strIdCol == null)
               strIdCol = key;
         }
         strValues = strValues.substring(0, strValues.length() - 1);
         strQuery = strQuery.substring(0, strQuery.length() - 1) + ") ";

         // INSERT query has NON_EXISTENT flag
         if ((insertFlags & INSERT_IF_NON_EXISTENT) == INSERT_IF_NON_EXISTENT)
         {
            strQuery += "SELECT " + strValues + " WHERE NOT EXISTS (SELECT * FROM " + strTable + " WHERE ";

            for (Entry<String, String> itemEntry : insertMap.entrySet())
            {
               if (!itemEntry.getValue().equals(DEFAULT_VALUE))
               {
                  if (!itemEntry.getValue().equals("NA"))
                     strQuery += itemEntry.getKey() + " = $$" + itemEntry.getValue() + "$$ AND ";
                  else
                     strQuery += itemEntry.getKey() + " = NULL AND ";
               }

            }
            strQuery = strQuery.substring(0, strQuery.length() - 4) + ")";
         }
         else
            strQuery += " VALUES (" + strValues + ")";

         // INSERT query has RETURNING flag
         if ((insertFlags & INSERT_RETURNING_ID) == INSERT_RETURNING_ID)
            strQuery += " RETURNING " + strIdCol;

         strQuery += ";";
         // create prepared statement
         PreparedStatement query = dbConnection.prepareStatement(strQuery);

         // assign given values
         int counter = 0;
         int skippedColumns = 0;
         for (Entry<String, String> item : insertMap.entrySet())
         {
            if (!item.getValue().equals(DEFAULT_VALUE))
            {
               if (colDataTypes[counter].equals("string"))
               {
                  if (item.getValue().equals(NULL_VALUE))
                     query.setNull(counter - skippedColumns + 1, Types.VARCHAR);
                  else
                     query.setString(counter - skippedColumns + 1, item.getValue());
               }
               else if (colDataTypes[counter].equals("integer"))
               {
                  if (!item.getValue().equals("NA"))
                     query.setInt(counter - skippedColumns + 1, Integer.parseInt(item.getValue()));
                  else
                     query.setNull(counter - skippedColumns + 1, Types.INTEGER);
               }

               else if (colDataTypes[counter].equals("float"))
                  query.setFloat(counter - skippedColumns + 1, Float.parseFloat(item.getValue()));
            }
            else
               skippedColumns++;
            counter++;
         }
         if ((insertFlags & INSERT_RETURNING_ID) == INSERT_RETURNING_ID)
            // execute query and return result
            res = query.executeQuery();
         else
            query.execute();
      }
      catch (SQLException e)
      {
         System.out.println(strQuery);
         e.printStackTrace();
      }
      catch (NumberFormatException e)
      {
         System.out.println(strQuery);
         System.out.println(insertMap);
         e.printStackTrace();
      }
      return res;
   }

   /**
    * Disconnecting from database
    * 
    * @return true, if closing the connection was successful
    */
   public boolean close()
   {
      try
      {
         this.dbConnection.close();
         return true;
      }
      catch (SQLException e)
      {
         // TODO Auto-generated catch block
         System.out.println("Error: connection could not be closed!");
         return false;
      }
   }
}
