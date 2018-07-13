package config;

public final class ALLConfig {
	
	public static final int SYNONYM_COUNT=100;
	
	public static final String FILE_NAME_CSV = "files/synonym_table.csv";
	
	public static final String FILE_NAME_ARFF = "files/similarity_scores.arff";
	public static final String ARFF_RELATION="@RELATION synonymtable";
	public static final String ARFF_ATTRIBUTE="@ATTRIBUTE ";
	public static final String ARFF_DATA="@DATA";
	public static final String ARFF_NUMERIC="numeric";
	public static final String JOB_ID="@ATTRIBUTE JobId string";
	public static final String EXTRO="@ATTRIBUTE Extroversion numeric";
	public static final String AGREE="@ATTRIBUTE Agreeableness numeric";
	public static final String CONSCI="@ATTRIBUTE Conscientiousness numeric";
	public static final String NEURO="@ATTRIBUTE Neuroticism numeric";
	public static final String OPEN="@ATTRIBUTE Openness numeric";
	public static final String NEW_LINE="\r\n";
}
