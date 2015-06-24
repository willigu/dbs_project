package dbs_java;

public class FilmActors {

	private String[] names;
	
	public FilmActors() {
		this.names = new String[]{};
	}
	
	public String[] getNames() {
		return names;
	}
	
	public void addName(String name) {
		String[] buffer = names;
		names = new String[names.length + 1];
		
		System.arraycopy(buffer, 0, names, 0, buffer.length);
		names[names.length -1] = name;
	}
	
	
	public int getLength() {
		return names.length;
	}
}
