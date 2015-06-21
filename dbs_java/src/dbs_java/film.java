package dbs_java;

public class film {
	public String title;
	public int year;
	public float rating;
	public int length;
	public String director;
	public String[] actors;
	public String[] genres;
	public int id;
	
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
	public film(String[] film) {
		this.title = film[1];
		this.year = Integer.parseInt(film[2]);
		this.rating = Float.parseFloat(film[3]);
		this.length = Integer.parseInt(film[5].split(" ")[0]);
		this.director = film[6];
		this.actors = film[7].split("|");
		this.genres = film[8].split("|");
	}
}
