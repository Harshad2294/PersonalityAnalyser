import functions.CSVParser;
import functions.DBReader;
import functions.LexicalAnalyzer;
import functions.Progress;

public class Initializer {
	
	private DBReader dbReader;
	private CSVParser csvParser;
	private LexicalAnalyzer lex;
	private Cleaner cleaner;
	private Progress progress;
	
	public Initializer() {
		dbReader = new DBReader();
		cleaner = new Cleaner();
		lex = new LexicalAnalyzer();
		progress = new Progress();
		csvParser = new CSVParser();
	}
	
	public CSVParser getCSVParser(){
		return csvParser;
	}

	public DBReader getDbReader() {
		return dbReader;
	}

	public LexicalAnalyzer getLex() {
		return lex;
	}
	
	public Cleaner getCleaner() {
		return cleaner;
	}
	
	public Progress getProgress() {
		return progress;
	}	
}