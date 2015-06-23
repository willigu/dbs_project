package dbs_java;

public class comm (DBHandler dbh) {
	private film[] obj = dbh.get_all_actornames();
	for (int i=0; i<obj.length; i++) {
		for (int j=0; ((j<3) and (j<film[i].actors.lenght)); j++) {
			system.out.println(film[i].actors[j]);
		}
	}
	

}
