import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

public class LemmaPOS
{
	private Properties props;
	private StanfordCoreNLP pipeline;
	private PrintStream err;
	
       	LemmaPOS()
    	{
       		err = System.err;
       		overrideErr();
    		// creates a StanfordCoreNLP object, with POS tagging, lemmatization, parsing
            props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse");
            pipeline = new StanfordCoreNLP(props);      		
    	}
       
       	public String processWord(String string)
       	{
       		String result,lemma = "",pos="";
       		// create an empty Annotation just with the given text
            Annotation document = new Annotation(string);
            // run all Annotators on this text
            pipeline.annotate(document);
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            for (CoreMap sentence : sentences) 
            {
            	// traversing the words in the current sentence
            	// a CoreLabel is a CoreMap with additional token-specific methods
            	for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class))
            	{
            		// this is the text of the token
            		lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
            		// this is the POS tag of the token
            		pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
            		// this is the NER label of the token
            	}
            }
            result=lemma+" "+pos;
            return result;
           }
       	
       	private void overrideErr()
       	{
       		System.setErr(new PrintStream(new OutputStream() {				
				@Override
				public void write(int b) throws IOException {}
			}));
       	}
       	
       	public void recoverErr() {
       		System.setErr(err);
       	}  	
}
