package config;

public final class DBConfig {
	public static final String DRIVER = "com.mysql.jdbc.Driver";
	public static final String URL = "jdbc:mysql://localhost:3306";
	public static final String USER_NAME = "root";
	public static final String PASSWORD = "root";
	public static final String DB_NAME = "/PersonalityAnalyser";
	public static final String GET_JOBS = "SELECT job_id FROM jobs";
	public static final String GET_PARAGRAPHS = "SELECT * FROM paragraphs WHERE job_id=?";
	public static final String GET_SELECTED_PARAGRAPHS = "select distinct job_id, descriptions from paragraphs,keywords,categories,person where keywords.paragraph_id=paragraphs.paragraph_id and keywords.category_id=categories.id and categories.p_id=person.person_id";
	public static final String FOR_PERSON = " and person.person_id=?";
	public static final String NOT_FOR_PERSON = " and person.person_id!=?";
}