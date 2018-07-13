package model;

import java.util.ArrayList;

public class JobAd{

	private String id;
	private String paragraph;
	private ArrayList<Token> tokens;

	public JobAd(String id,String paragraph){
		this.id = id;
		this.paragraph = paragraph;
		this.tokens = new ArrayList<>();
	}

	public String getId(){
		return id;
	}
	
	public void concatenateParagraph(String paragraph){
		this.paragraph+=" "+paragraph;
	}

	public String getParagraph(){
		return paragraph;
	}

	public void addToken(Token token){
		tokens.add(token);
	}
	
	public ArrayList<Token> getTokens(){
		return tokens;
	}

	public ArrayList<Token> getTokens(int[] sizes, ArrayList<PartsOfSpeech> PoS){
		ArrayList<Token> allTokens = new ArrayList<>();
		if (sizes!=null && PoS!=null){
			for(int size : sizes){
				for(PartsOfSpeech p: PoS){
					for(Token token : this.tokens){
						if(token.getTokenSize()==size && token.checkPartsOfSpeech(p)){
							allTokens.add(token);
						}
					}
				}
			}
		}
		else if(PoS==null){
			for(int size : sizes){
				for(Token token : this.tokens){
					if(token.getTokenSize()==size){
						allTokens.add(token);
					}
				}
			}
		}
		else if(sizes==null){
			for(PartsOfSpeech p: PoS){
				for(Token token : this.tokens){
					if(token.checkPartsOfSpeech(p)){
						allTokens.add(token);
					}
				}
			}
		}
		return allTokens;
	}

	public void setTokens(ArrayList<Token> tokens){
		this.tokens.addAll(tokens);
	}

	@Override
	public String toString() {
		return id+"] "+paragraph;
	}
	
	public void showAllTokens(){
		for(Token token:tokens){
			System.out.println(token.getToken()+" "+token.getPOS());
		}
	}
}
