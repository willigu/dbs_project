package dbs_java;

public class comm {
	
	private film[] obj;
	
	public comm (DBHandler dbh) {
		this.obj = dbh.get_all_actornames();
		for (int i=0; i<obj.length; i++) {
			for (int j=0; j<3 && j<obj[i].actors.length; j++) {
				System.out.println(obj[i].actors[j]);
			}
		}	
	}
	

}
