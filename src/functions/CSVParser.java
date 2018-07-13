package functions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import config.ALLConfig;

public class CSVParser {

	private HashMap<String, ArrayList<String>> synonymTable;
	private HashMap<String,ArrayList<String>> bigFive;

	public CSVParser() {
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void readCSV()
	{
		try
		{
			synonymTable = new HashMap<>();
			bigFive = new HashMap<>();
			String temp;
			BufferedReader csvReader = new BufferedReader(new FileReader(ALLConfig.FILE_NAME_CSV));
			while ((temp=csvReader.readLine())!=null) {
				StringTokenizer tokens = new StringTokenizer(temp, ",");
				boolean header = true;
				String tempHeader=null;
				String adj="";
				ArrayList<String> tempList = new ArrayList<>();
				while(tokens.hasMoreTokens()){
					if(header){
						String bigF = tokens.nextToken();
						tempHeader = tokens.nextToken();
						if(bigFive.containsKey(bigF)){
							ArrayList<String> adjectives = new ArrayList(bigFive.get(bigF));
							adjectives.add(tempHeader);
							bigFive.replace(bigF, adjectives);
						}
						else
						{
							ArrayList<String> tmp = new ArrayList();
							tmp.add(tempHeader);
							bigFive.put(bigF, tmp);
						}
						header = false;
					}
					else
					{
						adj = tokens.nextToken();
						tempList.add(adj);
					}
				}
				synonymTable.put(tempHeader, tempList);
			}
			csvReader.close();

		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<String> getKeywords()
	{
		return new ArrayList<>(synonymTable.keySet());
	}
	
	public String getKeywords(int i)
	{
		int cntr=0;
		for(ArrayList<String> r: bigFive.values())
		{
			for(String rr : r)
			{
				if(cntr==i)
					return rr;
				cntr++;
			}
		}
		return null;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList<String> getBigFive() {
		return new ArrayList(bigFive.keySet());
	}
	
	public double getScore(String adj) {
		for(String key:synonymTable.keySet()){
			if(adj.equals(key))
			{
				ArrayList<String> aa = synonymTable.get(key);
				return Double.parseDouble(aa.get(0));
			}
		}
		return 0.0;
	}
	
	public String getBigFive(String adjective)
	{
		for(String key: bigFive.keySet()){
			for(String adj : bigFive.get(key))
			{
				if(adj.equals(adjective))
				{
					return key;
				}
			}
		}
		return null;
	}
}