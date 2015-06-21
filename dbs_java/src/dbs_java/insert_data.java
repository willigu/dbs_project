package dbs_java;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.sql.*;

public class insert_data {
	private PreparedStatement stateActors;
	private PreparedStatement stateGenres;
	private PreparedStatement stateDirectors;
	private PreparedStatement stateFilms;
	private PreparedStatement stateFilmgenres;
	private PreparedStatement stateFilmcasts;
	private PreparedStatement SelectGenreId;
	private PreparedStatement SelectActorId;
	
	private DbBridge dbBridge;
	HashSet<String> hsActor = new HashSet<String>();
	HashSet<String> hsGenre = new HashSet<String>();
	HashSet<String> hsDirectors = new HashSet<String>();
	
	public insert_data(DbBridge dbBridge) throws SQLException {
		this.dbBridge = dbBridge;
		this.stateActors = dbBridge.dbConnection.prepareStatement("INSERT INTO Actors (ActorName) VALUES (?);");
		this.stateGenres = dbBridge.dbConnection.prepareStatement("INSERT INTO Genres (Genre) VALUES (?);");
		this.stateDirectors = dbBridge.dbConnection.prepareStatement("INSERT INTO Directors (Dirname) VALUES (?);");
		this.stateFilms = dbBridge.dbConnection.prepareStatement("INSERT INTO Films (FilmTitle, ReleaseYear, Rating, Length) VALUES (?,?,?,?) RETURNING FilmId;");
		this.stateFilmgenres = dbBridge.dbConnection.prepareStatement("INSERT INTO Filmgenres (FilmId,GenreId) VALUES (?,?);");
		this.stateFilmcasts = dbBridge.dbConnection.prepareStatement("INSERT INTO Filmcasts (FilmId,ActorId) VALUES (?,?);"); 
		PreparedStatement SelectActors = dbBridge.dbConnection.prepareStatement("SELECT ActorName FROM Actors;");
		PreparedStatement SelectGenres = dbBridge.dbConnection.prepareStatement("SELECT GenreName FROM Genres;");
		PreparedStatement SelectDirectors = dbBridge.dbConnection.prepareStatement("SELECT Dirname FROM Directors;");
		this.SelectGenreId = dbBridge.dbConnection.prepareStatement("SELECT GenreId FROM Genres WHERE GenreName=?;");
		this.SelectActorId = dbBridge.dbConnection.prepareStatement("SELECT ActorId FROM Actors WHERE ActorName=?;");
		ResultSet rs = SelectActors.executeQuery();
		while(rs.next()) {
			hsActor.add(rs.getString(1));
		}
		rs.close();
		rs = SelectGenres.executeQuery();
		while(rs.next()) {
			hsGenre.add(rs.getString(1));
		}
		rs.close();
		rs = SelectDirectors.executeQuery();
		while(rs.next()) {
			hsDirectors.add(rs.getString(1));
		}
		rs.close();
	}
	
	public void import_data() {
		String File ="./data/imdb_top100t_2015-06-18.csv";
		BufferedReader buffer = null;
		String zeile = "";
		String SplitChar ="	";
		
		
		try {
			buffer = new BufferedReader(new FileReader(File));
			
			while ((zeile = buffer.readLine()) != null) {
	 
				String[] film = zeile.split(SplitChar);
				
				film f = new film(film);
				
				for (String Actor:f.actors) {
					actor_import(Actor);
				}
				
				for (String Genre:f.genres) {
					genre_import(Genre);
				}
				
				
				directors_import(film[6]);
				
				film_import(f);
				/*
				 * film[0] IMDB ID
				 * film[1] Filmtitel
				 * film[2] Jahr
				 * film[3] Wertung
				 * film[4] ?
				 * film[5] Länge
				 * film[6] Regisseur
				 * film[7] Actors
				 * film[8] Genres
				 * 
				 */
			}
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (buffer != null) {
				try {
					buffer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	 
		System.out.println("Done");
	  }
	 
	public boolean actor_import(String Actor) throws SQLException {
		if (!hsActor.contains(Actor)) {
			hsActor.add(Actor);
			stateActors.setString(1, Actor);
			stateActors.execute();
			return true;
		}
		return false;
	}

	public boolean genre_import(String Genre) throws SQLException {
		if (!hsGenre.contains(Genre)) {
			hsGenre.add(Genre);
			stateGenres.setString(1, Genre);
			stateGenres.execute();
			return true;
		}
		return false;
	}
	
	public boolean directors_import(String Director) throws SQLException {
		if (!hsDirectors.contains(Director)) {
			hsDirectors.add(Director);
			stateDirectors.setString(1, Director);
			stateDirectors.execute();
			return true;
		}
		return false;
	}
	
	public void film_import(film f) throws SQLException {
		stateFilms.setString(1,f.title);
		stateFilms.setInt(2,f.year);
		stateFilms.setFloat(3,f.rating);
		stateFilms.setInt(4,f.length);
		ResultSet res = stateFilms.executeQuery();
		f.id = res.getInt(1);
		filmgenres_import(f);
		filmcasts_import(f);
	}
	
	public void filmgenres_import(film f) throws SQLException {
		for (String genre:f.genres) {
			stateFilmgenres.setInt(1, f.id);
			SelectGenreId.setString(1,genre);
			ResultSet res = SelectGenreId.executeQuery();
			res.next();
			stateFilmgenres.setInt(2, res.getInt(1));
			stateFilmgenres.execute();
		}
	}
	public void filmcasts_import(film f) throws SQLException {
		for (String actor:f.actors) {
			stateFilmcasts.setInt(1, f.id);
			SelectActorId.setString(1, actor);
			ResultSet res = SelectActorId.executeQuery();
			res.next();
			stateFilmcasts.setInt(2, res.getInt(1));
			stateFilmcasts.execute();
		}
	}
}