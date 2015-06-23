package dbs_java;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class comm {
	
	private film[] obj;
	
	private PreparedStatement GetActornameById;
	private PreparedStatement SelectAllActorsNamesperFilm;
	private PreparedStatement SelectNumberOfFilms;
	
	public comm (DBHandler dbh, DbBridge dbBridge) throws SQLException {
		
		this.SelectAllActorsNamesperFilm = dbBridge.dbConnection.prepareStatement("SELECT FilmId,ActorId FROM Filmcasts;");
		this.GetActornameById = dbBridge.dbConnection.prepareStatement("SELECT ActorName FROM Actors WHERE ActorId = ?;");
		this.SelectNumberOfFilms = dbBridge.dbConnection.prepareStatement("SELECT count(FilmId) FROM Films;");
		
		this.obj = get_all_actors();
		
		System.out.println("Das ist die erste Abfrage:Die ersten drei Hauptdarsteller der Filme");
		for (int i=0; i<obj.length; i++) {
			for (int j=0; j<3 && j<obj[i].actors.length; j++) {
				System.out.println(obj[i].actors[j]);
			}
		}	
		System.out.println("Das ist die zweite Abfrage: Bewertung der letzten drei Filme eines Regisseurs");
		
	}
	
	public String get_actorname_byid (int id) throws SQLException {
		GetActornameById.setInt(1,id);
		ResultSet res = getResults(GetActornameById);
		res.next();
		return res.getString(1);
	}
	
	
	private film[] get_all_actors() throws SQLException {
	// Actornames
		ResultSet res1 = getResults(SelectAllActorsNamesperFilm);
		res1.next(); 
		ResultSet res2 = getResults(SelectNumberOfFilms);
		res2.next();
		int numberoffilms = res2.getInt(1);
		int filmid,actorid;
		film[] films = new film[numberoffilms];
		int i = 0;
		String[] nActors;
		while(res1.next()) {
			filmid = res1.getInt(i);
			actorid = res1.getInt(i);
			nActors = new String[films[filmid].actors.length+1];
			System.arraycopy(films[filmid].actors, 0, nActors, 0, films[filmid].actors.length);
			nActors[nActors.length] = get_actorname_byid(actorid);
			films[filmid].actors = nActors;
			i++;
		}
	
	
		return films;
}

}
