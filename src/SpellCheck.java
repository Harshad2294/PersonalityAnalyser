import java.util.ArrayList;
import com.softcorporation.suggester.BasicSuggester;
import com.softcorporation.suggester.Suggestion;
import com.softcorporation.suggester.dictionary.BasicDictionary;
import com.softcorporation.suggester.util.SuggesterException;

public class SpellCheck {
	String suggesterDictionaryPath = "file://files/english.jar";
	BasicSuggester suggester;
	BasicDictionary suggesterDictionary;
	public SpellCheck() throws SuggesterException {
		suggester = new BasicSuggester();
		suggesterDictionary = new BasicDictionary(suggesterDictionaryPath);
		suggester.attach(suggesterDictionary);
	}

	@SuppressWarnings("rawtypes")
	public String getCorrectedLine(String word){
		ArrayList suggestions;
		try {
			suggestions = suggester.getSuggestions(word,1);
			if(suggestions.size()>0)
			{
				Suggestion suggestion = (Suggestion) suggestions.get(0);
				return suggestion.getWord();
			}
			else return null;
		} catch (SuggesterException e) {return null;}
	}
}
