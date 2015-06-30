package dbs_java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Scanner;

public class Program
{
   private String strFilename;
   private DbBridge dbBridge;

   /**
    * Executes the main program
    */
   public Program()
   {
      Properties properties = new Properties();
      try
      {
         properties.load(new FileInputStream("./data/config.conf"));
         System.out.println("Configuration file loaded...");
      }
      catch (IOException e1)
      {
         // TODO Auto-generated catch block
         e1.printStackTrace();
      }

      strFilename = "./data/imdb_top100t_2015-06-18.csv";
      dbBridge = new DbBridge(properties);

      // customize bridge settings
      System.out.println("What would you like to do?");
      Scanner in = new Scanner(System.in);
      while (true)
      {
         String strCmd = in.nextLine().trim().toLowerCase();

         switch (strCmd)
         {
            case "import":
               if (dbBridge.connect())
               {
                  System.out.println("Connected to database...");
                  importCSV();
                  System.out.println("Finished import!");
                  dbBridge.close();
                  System.out.println("Connection to database closed...");
               }
               else
                  System.exit(1);

               break;
            case "help":
               System.out.println("commands: import, exit");
               break;
            case "exit":
               in.close();
               System.exit(0);
               break;
         }

      }

   }

   /**
    * Imports a given CSV file
    */
   public void importCSV()
   {
      BufferedReader fileBuffer = null;
      String strLine = "";

      try
      {
         // create file buffer
         fileBuffer = new BufferedReader(new FileReader(strFilename));

         // loop through each line and extract information
         while ((strLine = fileBuffer.readLine()) != null)
         {
            String[] rawMovieString = strLine.split("\t");
            LinkedHashMap<String, String> moviesInstance = new LinkedHashMap<String, String>();

            // put original id
            moviesInstance.put("FilmId", rawMovieString[0].substring(2, rawMovieString[0].length()));

            // put movie title to it.
            moviesInstance.put("FilmTitle", rawMovieString[1]);

            // put movie year
            moviesInstance.put("ReleaseYear", rawMovieString[2]);

            // put rating
            moviesInstance.put("Rating", rawMovieString[3]);

            // put length
            moviesInstance.put("Length", rawMovieString[5].split(" ")[0]);

            // movie directors
            String[] strMovieDirectors = rawMovieString[6].split("\\|");
            ArrayList<LinkedHashMap<String, String>> movieDirectors = decomposeMultivaluedAttributes(strMovieDirectors,
                  "Directors", new String[] {"DirId", "DirName"}, new String[] {"EntryId", "FilmId", "DirId"});

            // move actors
            String[] strMovieActors = rawMovieString[7].split("\\|");
            ArrayList<LinkedHashMap<String, String>> movieActors = decomposeMultivaluedAttributes(strMovieActors,
                  "Actors", new String[] {"ActorId", "ActorName"}, new String[] {"EntryId", "FilmId", "ActorId"});

            // movie genres
            String[] strMovieGenres = rawMovieString[8].split("\\|");
            ArrayList<LinkedHashMap<String, String>> movieGenres = decomposeMultivaluedAttributes(strMovieGenres,
                  "Genres", new String[] {"GenreId", "GenreName"}, new String[] {"EntryId", "FilmId", "GenreId"});

            // execute import
            try
            {
               ResultSet result = dbBridge.insert("Films", moviesInstance, new String[] {"integer", "string",
                     "integer", "float", "integer"}, DbBridge.INSERT_IF_NON_EXISTENT | DbBridge.INSERT_RETURNING_ID);
               if (!result.next())
                  System.out.println("Error: Possibly duplication!");
               else
               {
                  String filmId = String.valueOf(result.getInt(1));
                  // do FilmDirector table
                  for (LinkedHashMap<String, String> filmDirItem : movieDirectors)
                  {
                     // replace default value in FilmId column
                     filmDirItem.replace("FilmId", filmId);

                     // do insert
                     dbBridge.insert("FilmDirectors", filmDirItem, new String[] {"integer", "integer", "integer"},
                           DbBridge.INSERT_NO_OPTION);
                  }
                  // do Filmcasts table
                  for (LinkedHashMap<String, String> filmActorItem : movieActors)
                  {
                     // replace default value in FilmId column
                     filmActorItem.replace("FilmId", filmId);

                     // do insert
                     dbBridge.insert("Filmcasts", filmActorItem, new String[] {"integer", "integer", "integer"},
                           DbBridge.INSERT_NO_OPTION);
                  }
                  // do FilmGenre table
                  for (LinkedHashMap<String, String> filmGenreItem : movieGenres)
                  {
                     // replace default value in FilmId column
                     filmGenreItem.replace("FilmId", String.valueOf(result.getInt(1)));

                     // do insert
                     dbBridge.insert("FilmGenres", filmGenreItem, new String[] {"integer", "integer", "integer"},
                           DbBridge.INSERT_NO_OPTION);
                  }
               }
            }
            catch (SQLException e)
            {
               // TODO Auto-generated catch block
               e.printStackTrace();
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   /**
    * Decomposes a multivalue attribute
    * 
    * @param attributeArray
    *           the source attribute array
    * @param strTable
    *           the name of the table for the main attribute
    * @param attributeTable
    *           the columns of the table for the main attribute
    * @param attributeFilmTable
    *           the columns of the new cross referencing table
    * @return
    */
   public ArrayList<LinkedHashMap<String, String>> decomposeMultivaluedAttributes(String[] attributeArray,
         String strTable, String[] attributeTable, String[] attributeFilmTable)
   {
      ArrayList<LinkedHashMap<String, String>> movieAttributes = new ArrayList<LinkedHashMap<String, String>>();
      for (String attribute : attributeArray)
      {
         if (attribute.toLowerCase().equals("['na']"))
            continue;

         // mapping for the main attribute table
         LinkedHashMap<String, String> attributeHashMap = new LinkedHashMap<String, String>();

         // mapping for the cross referencing table
         LinkedHashMap<String, String> crossRefTable = new LinkedHashMap<String, String>();

         attributeHashMap.put(attributeTable[0], DbBridge.DEFAULT_VALUE);

         attributeHashMap.put(attributeTable[1], attribute);

         // insert genre into Genres table
         ResultSet result = dbBridge.insert(strTable, attributeHashMap, new String[] {"integer", "string"},
               DbBridge.INSERT_IF_NON_EXISTENT | DbBridge.INSERT_RETURNING_ID);

         // determine genreId
         int attrId = -1;
         try
         {
            if (result.next())
               attrId = result.getInt(1);
            else
            {
               ResultSet resultGetId = dbBridge.x("SELECT " + attributeTable[0] + " FROM " + strTable + " WHERE "
                     + attributeTable[1] + " = $$" + attribute + "$$;");
               if (resultGetId.next())
                  attrId = resultGetId.getInt(1);
            }
         }
         catch (SQLException e)
         {
            e.printStackTrace();
         }

         // build query mapping
         crossRefTable.put(attributeFilmTable[0], DbBridge.DEFAULT_VALUE);
         crossRefTable.put(attributeFilmTable[1], DbBridge.DEFAULT_VALUE);
         crossRefTable.put(attributeFilmTable[2], String.valueOf(attrId));

         // add to query list
         movieAttributes.add(crossRefTable);
      }

      return movieAttributes;
   }

   public static void main(String[] args)
   {

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

      /*
      // Import our data
      try {
      	DBHandler dbh = new DBHandler(dbBridge);
      	dbh.import_csv("./data/imdb_top100_2015-06-18.csv");
      } catch (SQLException e) {
      	// TODO Auto-generated catch block
      	e.printStackTrace();
      }
      */

      Program p = new Program();
   }

}
