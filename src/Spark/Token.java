package Spark;
import java.io.Serializable;

public class Token implements Serializable {
	public String text;
	public String ner;
	public String lemma;
	
	public Token(String text, String ner, String lemma) {
		super();
		this.text = text;
		this.ner = ner;
		this.lemma = lemma;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getNer() {
		return ner;
	}
	public void setNer(String ner) {
		this.ner = ner;
	}
	public String getLemma() {
		return lemma;
	}
	public void setLemma(String lemma) {
		this.lemma = lemma;
	}

	
	
}
