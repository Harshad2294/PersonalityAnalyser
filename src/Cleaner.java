import java.util.ArrayList;
import java.util.HashMap;
import com.softcorporation.suggester.util.SuggesterException;
import model.JobAd;
import model.PartsOfSpeech;
import model.Token;

public class Cleaner 
{
	@SuppressWarnings("unused")
	private JobAd ad;
	@SuppressWarnings("unused")
	private ArrayList<Token> tokens;
	private SpellCheck spellCheck;
	private LemmaPOS lemmaPOS;

	public Cleaner() 
	{
		try {
			spellCheck=new SpellCheck();
			lemmaPOS=new LemmaPOS();
		} catch (SuggesterException e) {e.printStackTrace();}
	}
	public JobAd clean(JobAd ad)
	{
		tokens = new ArrayList<>();
		String temp,lemma,pos;
		String paragraph= ad.getParagraph();
		String[] words=paragraph.replaceAll("[^\\w\\s ]", "").toLowerCase().split("\\s+");
		for (String string : words)
		{
			string=spellCheck.getCorrectedLine(string);
			if(string!=null)
			{
				temp= lemmaPOS.processWord(string);
				String[] tempArray=temp.split(" ");
				if(tempArray.length==2)
				{
					lemma=tempArray[0];
					pos=tempArray[1];
					HashMap<String, PartsOfSpeech> hs = new HashMap<>();
					hs.put(lemma, allocatePOS(pos));
					ad.addToken(new Token(hs));
				}
			}
		}	
		return ad;
	}

	public void recoverErr(){
		lemmaPOS.recoverErr();
	}

	private PartsOfSpeech allocatePOS(String pos) {
		if(pos.equals("NN")||pos.equals("NNS")||pos.equals("NNP")||pos.equals("NNPS"))
		{
			return PartsOfSpeech.NOUN;
		}
		else if(pos.equals("PRP")||pos.equals("PRP$")||pos.equals("WP")||pos.equals("WP$"))
		{
			return PartsOfSpeech.PRONOUN;
		}
		else if(pos.equals("VB")||pos.equals("VBD")||pos.equals("VBG")||pos.equals("VBN")||pos.equals("VBP")||pos.equals("VBZ"))
		{
			return PartsOfSpeech.VERB;
		}
		else if(pos.equals("JJ")||pos.equals("JJR")||pos.equals("JJS"))
		{
			return PartsOfSpeech.ADJECTIVE;
		}
		else if(pos.equals("RB")||pos.equals("RBR")||pos.equals("RBS"))
		{
			return PartsOfSpeech.ADVERB;
		}
		else if(pos.equals("IN"))
		{
			return PartsOfSpeech.PREPOSITION;
		}
		else if(pos.equals("CC"))
		{
			return PartsOfSpeech.CONJUNCTION;
		}
		else if(pos.equals("UH"))
		{
			return PartsOfSpeech.INTERJECTION;
		}
		else
			return PartsOfSpeech.OTHERS;
	}

}
