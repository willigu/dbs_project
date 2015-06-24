package dbs_java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class comm {
	
	private FilmActors[] obj;
	
	private PreparedStatement GetActornameById;
	private PreparedStatement SelectAllActorsNamesperFilm;
	private PreparedStatement SelectNumberOfFilms;
	private PreparedStatement DirectorList;
	private PreparedStatement lastratings;
	private PreparedStatement difference;
	private PreparedStatement allRatings;
	private PreparedStatement neuesterFilmTitle;
	private PreparedStatement neuesterFilmDir;
	private PreparedStatement bestesrating;
	private PreparedStatement selrelyear;
	
	private DBHandler dbh;
	
	public comm (DBHandler dbh, DbBridge dbBridge) throws SQLException {
		
		this.SelectAllActorsNamesperFilm = dbBridge.dbConnection.prepareStatement("SELECT FilmId,ActorId FROM Filmcasts;");
		this.GetActornameById = dbBridge.dbConnection.prepareStatement("SELECT ActorName FROM Actors WHERE ActorId = ?;");
		this.SelectNumberOfFilms = dbBridge.dbConnection.prepareStatement("SELECT count(FilmId) FROM Films;");
		this.DirectorList = dbBridge.dbConnection.prepareStatement("SELECT DirId,DirName FROM Directors;");
		this.lastratings = dbBridge.dbConnection.prepareStatement("SELECT Rating FROM Films WHERE Releaseyear = ?;");
		this.selrelyear =  dbBridge.dbConnection.prepareStatement("SELECT ReleaseYear FROM Films WHERE DirId=? ORDER BY ReleaseYear DESC LIMIT 3;");
		this.difference = dbBridge.dbConnection.prepareStatement("SELECT (MAX(ReleaseYear) - MIN(ReleaseYear)) FROM Films WHERE DirId=?;");
		this.allRatings = dbBridge.dbConnection.prepareStatement("SELECT Rating FROM Films ORDER BY ReleaseYear DESC LIMIT 5;");
		this.neuesterFilmTitle = dbBridge.dbConnection.prepareStatement("SELECT FilmTitle FROM Films ORDER BY ReleaseYear DESC LIMIT 1;");
		this.neuesterFilmDir = dbBridge.dbConnection.prepareStatement("SELECT DirName FROM Directors WHERE DirId=(SELECT DirId FROM Films ORDER BY ReleaseYear DESC LIMIT 1);");
		this.bestesrating = dbBridge.dbConnection.prepareStatement("SELECT FilmTitle FROM Films WHERE Rating=(SELECT MAX(Rating) FROM Films);");
		
		this.dbh=dbh;
		this.obj = get_all_actors();
		
		System.out.println("Das ist die erste Abfrage:Die ersten drei Hauptdarsteller der Filme");
		int filmnumber;
		for (int i=0; i<obj.length; i++) {
			filmnumber = i+1;
			System.out.println("Die ersten 3 Hauptdarsteller vom "+filmnumber+". Film:");
			for (int j=0; j<3 && j<obj[i].getLength(); j++) {
				System.out.println(obj[i].getNames()[j]);
			}
		}	
		
		System.out.println("Das ist die zweite Abfrage: Bewertung der letzten drei Filme eines Regisseurs");
		ResultSet res = dbh.getResults(DirectorList); 
		ResultSet res2,res3;
		int id;
		String name;
		float[] rating = new float[3];
		int fid;
		while (res.next()) {
			id = res.getInt(1);
			name = res.getString(2);
			System.out.println("Director: "+ name);
			selrelyear.setInt(1,id);
			res3 = selrelyear.executeQuery();
			for (int k=0; k<3;k++) {
				if (res3.next()) {
					fid = res3.getInt(1);
					lastratings.setInt(1, fid);
					res2 = dbh.getResults(lastratings);
					res2.next();
					rating[k] = res2.getFloat(1); //DANGER: if a director has less than 3 ratings -> kaboom
				}
				
			}
			if (rating.length<3) {
				System.out.println("Nicht genug Daten");
			} else{
				System.out.println(rating[0]+","+rating[1]+","+rating[2]);
			}
		}
		
		System.out.println("Dast ist die dritte Abfrage: Abstand der Filme in Jahren eines Regisseurs");
		res = dbh.getResults(DirectorList);
		while (res.next()) {
			id = res.getInt(1);
			name = res.getString(1);
			System.out.println("Director: "+ name);
			difference.setInt(1,id);
			res2 = dbh.getResults(difference);
			res2.next();
			System.out.println(res2.getInt(1));
		}
		
		System.out.println("Das ist die vierte Abfrage: Durchschnittliche Steigung der Bewertungen der letzten f�nf Filme");
		res = dbh.getResults(allRatings);
		res.next();
		float[] fiveratings = new float[5];
		int rcount=0;
		while ((res.next())&&rcount<5) {
			fiveratings[rcount] = res.getFloat(1);
			rcount++;
		}
		float steigung = ((fiveratings[4] - fiveratings[3]) + (fiveratings[3] - fiveratings[2]) + (fiveratings[2] - fiveratings[1]) + (fiveratings[1] - fiveratings[0]))/5;
		System.out.println("Die durchschnittliche Steigung der letzten 5 Ratings ist: "+ steigung);
		
		System.out.println("Das ist die erste zus�tzliche Abfrage: Wie hei�t der neueste Film?");
		res = dbh.getResults(neuesterFilmTitle);
		res.next();
		name = res.getString(1);
		System.out.println("Der neueste Film hei�t: "+name);
		
		System.out.println("Das ist die zweite zus�tzliche Abfrage: Wie hei�t der Director des neuesten Films?");
		res = dbh.getResults(neuesterFilmDir);
		res.next();
		name = res.getString(1);
		System.out.println("Der Director des neuesten Films hei�t: "+ name);
		
		System.out.println("Das ist die dritte zus�tzliche Abfrage: Wie hei�t der Film mit dem h�chsten Rating?");
		res = dbh.getResults(bestesrating);
		res.next();
		name = res.getString(1);
		System.out.println("Ein Film mit dem h�chsten Rating hei�t: "+ name);
	}
	
	private String get_actorname_byid (int id) throws SQLException {
		GetActornameById.setInt(1,id);
		ResultSet res = dbh.getResults(GetActornameById);
		res.next();
		return res.getString(1);
	}
	
	
	private FilmActors[] get_all_actors() throws SQLException {
	// Actornames
		ResultSet res1 = dbh.getResults(SelectAllActorsNamesperFilm);
		res1.next(); 
		ResultSet res2 = dbh.getResults(SelectNumberOfFilms);
		res2.next();
		int numberoffilms = res2.getInt(1);
		int filmid,actorid;
		
		FilmActors[] filmactors = new FilmActors[numberoffilms]; 
		
		for (int j=0;j<numberoffilms;j++) {
			filmactors[j] = new FilmActors();
		}
		while(res1.next()) {
			filmid = res1.getInt(1);
			actorid = res1.getInt(2);
			filmactors[filmid-1].addName(get_actorname_byid(actorid));
		}
		return filmactors;
	}
}
