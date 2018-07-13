package functions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import config.ALLConfig;

public class ARFFParser {
	private BufferedWriter arffWriter;

	public ARFFParser() {
		try {
			arffWriter = new BufferedWriter(new FileWriter(ALLConfig.FILE_NAME_ARFF));
		} catch (IOException e) {e.printStackTrace();}
	}
	
	public void writeARFF(HashMap<String, ArrayList<Double>> bigFiveScores, ArrayList<HashMap<String, HashMap<String,Double>>> jobScores, CSVParser csvParser)
	{
		try {
			arffWriter.append(ALLConfig.ARFF_RELATION+ALLConfig.NEW_LINE+ALLConfig.NEW_LINE);
			
			arffWriter.append(ALLConfig.JOB_ID+ALLConfig.NEW_LINE);
			arffWriter.append(ALLConfig.EXTRO+ALLConfig.NEW_LINE);
			arffWriter.append(ALLConfig.AGREE+ALLConfig.NEW_LINE);
			arffWriter.append(ALLConfig.CONSCI+ALLConfig.NEW_LINE);
			arffWriter.append(ALLConfig.NEURO+ALLConfig.NEW_LINE);
			arffWriter.append(ALLConfig.OPEN+ALLConfig.NEW_LINE);
			for(String adjs : csvParser.getKeywords())
			{
				adjs = adjs.replaceAll(" ", "-");
				arffWriter.append(ALLConfig.ARFF_ATTRIBUTE+adjs+" "+ALLConfig.ARFF_NUMERIC+ALLConfig.NEW_LINE);
			}
			arffWriter.append(ALLConfig.NEW_LINE);
			
			arffWriter.append(ALLConfig.ARFF_DATA+ALLConfig.NEW_LINE);
			
			String line="";
			Set<String> lines = new HashSet<>();
			for(String jobId: bigFiveScores.keySet())
			{
				line+=jobId+",";
				for(double bfScores : bigFiveScores.get(jobId))
				{
					line+=bfScores+",";
				}
				for(HashMap<String, HashMap<String,Double>> adjScores : jobScores) {
					if(adjScores.get(jobId)==null)
						continue;
					HashMap<String,Double> scores = new HashMap<>(adjScores.get(jobId));
					int cntrr=0;
					for(String adj : scores.keySet())
					{
						if(cntrr==scores.size()-1)
						{
							line+=scores.get(adj)+ALLConfig.NEW_LINE;
						}
						else
							line+=scores.get(adj)+",";
						cntrr++;
					}
					break;
				}
				lines.add(line);
			}
			
			
			long max=0;
			String line2="";
			for(String line1:lines)
			{
				int cnt=line1.length();
				if(max<cnt)
				{
					max=cnt;
					line2=line1;
				}
			}
			arffWriter.append(line2);
			arffWriter.close();
			
		} catch (IOException e) {e.printStackTrace();}
	}
}
