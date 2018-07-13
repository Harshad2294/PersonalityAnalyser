import java.util.ArrayList;
import java.util.HashMap;

import functions.ARFFParser;
import functions.CSVParser;
import functions.DBReader;
import functions.LexicalAnalyzer;
import functions.Progress;
import functions.SynonymOps;
import model.JobAd;
import model.Token;

public class Main {

	public static DBReader dbr;
	public static LexicalAnalyzer lex;
	public static Cleaner cleaner;
	public static Progress progress;
	public static CSVParser csvParser;
	public static Initializer initializer;
	public static SynonymOps syno;
	public static HashMap<String, ArrayList<String>> synonyms;
	public static ArrayList<HashMap<String, HashMap<String,Double>>> jobScores;
	public static HashMap<String, ArrayList<Double>> bigFiveScores;

	public static void initialize(){
		dbr = initializer.getDbReader();
		lex = initializer.getLex();
		cleaner = initializer.getCleaner();
		progress = initializer.getProgress();
		syno = new SynonymOps();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws InterruptedException {
		//long start = System.currentTimeMillis();
		initializer = new Initializer();
		initialize();
		ArrayList<JobAd> jobs = new ArrayList(readFromDB());
		Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				readFromCSV();
				getAllSynonyms();
			}
		});
		t.start();
		jobs = new ArrayList(cleanParagraphs(jobs));
		t.join();
		lexicalAnalyser(jobs);
		calculateBigFive(jobs);
		writeARFF();
		System.out.println("Program executed successfully.");
		//long end = System.currentTimeMillis();
		//System.out.println((end-start)/1000+"secs");
	}

	private static void calculateBigFive(ArrayList<JobAd> jobs) {
		progress.printProgress("Calculating Big Five personality scores");
		bigFiveScores = new HashMap<>();
		double[] scores = new double[csvParser.getBigFive().size()];
		int cntr=0;
		for(HashMap<String, HashMap<String,Double>> abc:jobScores)
		{
			cntr=0;
			String jobId="";
			for(int i=0;i<scores.length;i++)	scores[i]=0.0;
			for(String keyId: abc.keySet())jobId=keyId;
			for(String bigFive:csvParser.getBigFive())
			{
				for(HashMap<String, Double> def : abc.values())
				{
					for(String ghi : def.keySet())
					{
						if(bigFive.equals(csvParser.getBigFive(ghi)))
						{
							scores[cntr]+=def.get(ghi);
						}
					}
				}
				cntr++;
			}
			ArrayList<Double> sc = new ArrayList<>();
			for(double scr:scores) sc.add(scr);
			bigFiveScores.put(jobId,sc);
		}
		progress.stopProgress();
	}

	private static void writeARFF() {
		progress.printProgress("Writing similarity scores to file");
		new ARFFParser().writeARFF(bigFiveScores,jobScores,csvParser);
		progress.stopProgress();
	}

	private static void getAllSynonyms() {
		synonyms = new HashMap<>();
		ArrayList<String> keywords = new ArrayList<>(csvParser.getKeywords());
		for(String key: keywords){
			try
			{
				ArrayList<String> tt = new ArrayList<>();
				tt.add(key);
				//synonyms.put(key, tt);
				synonyms.put(key, syno.findSimilar(key));
			}catch (Exception e) {e.printStackTrace();
			}
		}
	}

	///////////////////////////		Lexical Analyzer	///////////////////////////
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void lexicalAnalyser(ArrayList<JobAd> jobs){
		System.out.println("Lexical Analysis progress");
		progress.updateTasks(jobs.size());
		jobScores = new ArrayList();
		for(JobAd job : jobs)
		{
			if(job!=null) {
				ArrayList<Double> adjectiveScore = new ArrayList<>(synonyms.keySet().size());
				for(String keyword : synonyms.keySet()){
					double max=0.0;

					for(Token token : job.getTokens())
					{
						double score = lex.runJCN(token.getToken().trim(),keyword);
						if(max<score)
							max=score;
					}

					for(Token token : job.getTokens()){
						for(String syns : synonyms.get(keyword)){
							double score = lex.runJCN(token.getToken().trim(),syns);
							if(max<score)
								max=score;
						}
					}
					adjectiveScore.add(max);
				}
				HashMap<String, HashMap<String,Double>> tempHM = new HashMap();
				HashMap<String,Double> tmp = new HashMap<>();
				for(int j=0;j<adjectiveScore.size();j++)
				{
					tmp.put(csvParser.getKeywords(j), adjectiveScore.get(j));
				}
				tempHM.put(job.getId(), new HashMap<>(tmp));
				jobScores.add(tempHM);
				progress.update();
			}
		}
		System.out.println();
	}
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////		Reading from CSV	///////////////////////	
	private static void readFromCSV() {
		csvParser = initializer.getCSVParser();
		csvParser.readCSV();
	}
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////		Reading from DB 	///////////////////////	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ArrayList<String> readFromDB()
	{
		progress.printProgress("Reading from database");
		ArrayList<String> tempjobs = new ArrayList(dbr.getAllJobParagraphs(3, true));
		dbr.closeConnection();
		progress.stopProgress();
		return tempjobs;
	}
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////		Cleaning	///////////////////////////////	
	private static ArrayList<JobAd> cleanParagraphs(ArrayList<JobAd> jobs)
	{
		System.out.println("Cleaning progress");
		ArrayList<JobAd> newJobs = new ArrayList<>();
		progress.updateTasks(jobs.size());
		double s1 = jobs.size()/2;
		int s2 = jobs.size()- (int)(s1);
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=0;i<s2;i++)
				{
					JobAd tempJob = cleaner.clean(jobs.get(i));
					progress.update();
					newJobs.add(tempJob);
				}
			}
		});
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=s2;i<jobs.size();i++)
				{
					JobAd tempJob = cleaner.clean(jobs.get(i));
					progress.update();
					newJobs.add(tempJob);
				}
			}
		});		
		t1.start();
		t2.start();
		try
		{
		t1.join();
		t2.join();
		}catch (Exception e) {e.printStackTrace();}
		System.out.println();
		cleaner.recoverErr();
		cleaner = null;
		System.gc();
		return newJobs;
	}
	///////////////////////////////////////////////////////////////////////////////
}
