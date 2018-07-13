package model;

import java.util.HashMap;

public class Token {
	
	private HashMap<String, PartsOfSpeech> token_and_partsOfSpeech;
	
	public Token(HashMap<String, PartsOfSpeech> token_and_partsOfSpeech) {
		this.token_and_partsOfSpeech = new HashMap<>();
		this.token_and_partsOfSpeech.putAll(token_and_partsOfSpeech);
	}

	public HashMap<String, PartsOfSpeech> getToken_and_partsOfSpeech() {
		return token_and_partsOfSpeech;
	}
	
	public boolean checkPartsOfSpeech(PartsOfSpeech PoS){
		for(PartsOfSpeech check:token_and_partsOfSpeech.values()){
			if (check==PoS){
				return true;
			}
		}
		return false;
	}
	
	public String getToken(){
		String tok="";
		for(String check:token_and_partsOfSpeech.keySet()){
			tok+=check+" ";
		}
		return tok;
	}
	
	public String getPOS(){
		String tok="";
		for(String check:token_and_partsOfSpeech.keySet()){
			tok+=token_and_partsOfSpeech.get(check)+" ";
		}
		return tok;
	}
	
	public int getTokenSize(){
		return token_and_partsOfSpeech.size();
	}
}