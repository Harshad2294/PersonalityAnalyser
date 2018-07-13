package functions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import org.json.*;
import config.ALLConfig;

public class SynonymOps {

	public SynonymOps() {
	}

	/**
	 * Returns a list of similar words to the word/phrase supplied.
	 * @param word A word of phrase.
	 * @return A list of similar words.
	 */
	public ArrayList<String> findSimilar(String word) {
		ArrayList<String> syns = new ArrayList<>();
		String s = word.replaceAll(" ", "+");
		JSONObject jsonObj = new JSONObject("{\"words\":"+getJSON("http://api.datamuse.com/words?rel_syn="+s)+"}");
		JSONArray arr = jsonObj.getJSONArray("words");
		for (int i = 0; i < ALLConfig.SYNONYM_COUNT && i<arr.length(); i++)
		{
			syns.add(arr.getJSONObject(i).getString("word"));
		}
		return syns;
	}

	/**
	 * Query a URL for their source code.
	 * @param url The page's URL.
	 * @return The source code.
	 */
	private String getJSON(String url) {
		URL datamuse;
		URLConnection dc;
		StringBuilder s = null;
		try {
			datamuse = new URL(url);
			dc = datamuse.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(dc.getInputStream(), "UTF-8"));
			String inputLine;
			s = new StringBuilder();
			while ((inputLine = in.readLine()) != null)
				s.append(inputLine);
			in.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return s != null ? s.toString() : null;
	}
}