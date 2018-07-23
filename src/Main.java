import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
	public static Initializer controller;
	public static SynonymOps syno;
	public static HashMap<String, ArrayList<String>> synonyms;
	public static ArrayList<HashMap<String, HashMap<String,Double>>> jobScores;
	public static HashMap<String, ArrayList<Double>> bigFiveScores;

	public static void initialize(){
		dbr = controller.getDbReader();
		lex = new LexicalAnalyzer();
		cleaner = controller.getCleaner();
		progress = controller.getProgress();
		syno = new SynonymOps();
	}

	public static void main(String[] args) throws InterruptedException {
		long start = System.currentTimeMillis();
		controller = new Initializer();
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
		t.stop();
		lexicalAnalyser(jobs);
		calculateBigFive(jobs);
		writeARFF();
		System.out.println("Program executed successfully.");
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000+"secs");
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
		//progress.printProgress("Downloading synonyms for keywords");
		synonyms = new HashMap<>();
		ArrayList<String> keywords = new ArrayList<>(csvParser.getKeywords());
		for(String key: keywords){
			try
			{
				ArrayList<String> tt = new ArrayList<>();
				tt.add(key);
				synonyms.put(key, syno.findSimilar(key));
			}catch (Exception e) {e.printStackTrace();
			}
		}
		//progress.stopProgress();
	}

	///////////////////////////		Lexical Analyzer	///////////////////////////
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		csvParser = controller.getCSVParser();
		csvParser.readCSV();
	}
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////		Reading from DB 	///////////////////////	
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
		int s1 = (int)(jobs.size()/4);
		int rem = (int)(jobs.size()%4);
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=0;i<s1;i++)
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
				for(int i=s1;i<(2*s1);i++)
				{
					JobAd tempJob = cleaner.clean(jobs.get(i));
					progress.update();
					newJobs.add(tempJob);
				}
			}
		});
		Thread t3 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=2*s1;i<(3*s1);i++)
				{
					JobAd tempJob = cleaner.clean(jobs.get(i));
					progress.update();
					newJobs.add(tempJob);
				}
			}
		});
		Thread t4 = new Thread(new Runnable() {
			@Override
			public void run() {
				for(int i=3*s1;i<(4*s1+rem);i++)
				{
					JobAd tempJob = cleaner.clean(jobs.get(i));
					progress.update();
					newJobs.add(tempJob);
				}
			}
		});
		
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		try
		{
			t1.join();
			t1.interrupt();
			t2.join();
			t2.interrupt();
			t3.join();
			t3.interrupt();
			t4.join();
			t4.interrupt();
		}catch (Exception e) {e.printStackTrace();}
		System.out.println();
		cleaner.recoverErr();
		cleaner = null;
		System.gc();
		return newJobs;
	}
	///////////////////////////////////////////////////////////////////////////////
}
