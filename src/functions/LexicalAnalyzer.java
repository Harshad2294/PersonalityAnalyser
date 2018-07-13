package functions;

import edu.cmu.lti.jawjaw.JAWJAW;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.JiangConrath;


public class LexicalAnalyzer extends JAWJAW {
	private ILexicalDatabase db;
	private RelatednessCalculator jcn;

	public LexicalAnalyzer() {
		db = new NictWordNet();
		jcn = new JiangConrath(db);
	}
	
	/**
	 * Calculates the Jiang-Conrath distance score between two synsets.
	 * Following definition is cited from (Budanitsky and Hirst, 2001).
	 * <blockquote>
	 * Jiang–Conrath: Jiang and Conrath’s (1997) approach
	 * also uses the notion of information content, but in the
	 * form of the conditional probability of encountering an instance
	 * of a child-synset given an instance of a parent synset.
	 * Thus the information content of the two nodes, as
	 * well as that of their most specific subsumer, plays a part.
	 * Notice that this formula measures semantic distance, the
	 * inverse of similarity.
	 * <div style="padding:20px"><code>dist<sub>JS</sub>(c<sub>1</sub>, c<sub>2</sub>) = 2 * log( p(lso(c<sub>1</sub>, c<sub>2</sub>)) ) - ( log(p(c<sub>1</sub>))+log(p(c<sub>2</sub>) ) ).</code></div>
	 * </blockquote>
	 * 
	 * @param word1 word lemma in Japanese or English
	 * @param word2 word lemma in Japanese or English
	 * @return semantic relatedness of two words
	 */
	public double runJCN( String word1, String word2 ) {
		return jcn.calcRelatednessOfWords( word1, word2 );
	}
}
