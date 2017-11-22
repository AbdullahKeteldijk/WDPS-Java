import java.io.Serializable;

public class CustomWarcRecord implements Serializable {
	public String recordID;
	public String content;
	
	public CustomWarcRecord(String rec,String con) {
		this.recordID = rec;
		this.content = con;
	}
	public String getRecordID() {
		return recordID;
	}
	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	
	
	
}
