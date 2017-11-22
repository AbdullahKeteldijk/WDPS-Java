import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.collections.map.HashedMap;

public class AnnotatedRecord implements Serializable {
	public String recordID;
	public ArrayList<Token> tokens;
	
	public AnnotatedRecord(String recordID, ArrayList<Token> tokens) {
		super();
		this.recordID = recordID;
		this.tokens = tokens;
	}
	
	public String getRecordID() {
		return recordID;
	}
	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}
	public ArrayList<Token> getTokens() {
		return tokens;
	}
	public void setTokens(ArrayList<Token> tokens) {
		this.tokens = tokens;
	}
	
	
}
