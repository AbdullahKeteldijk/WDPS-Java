package Spark;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class test {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("language", "english");
		props.setProperty("annotators", "tokenize, pos, lemma, ner");
		props.setProperty("ner.useSUTime", "false");
		props.setProperty("ner.applyNumericClassifiers", "false");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
	
		Annotation documentSentences = new Annotation("Hello World. This is Kevin");
		pipeline.annotate(documentSentences);
	
		
		List<CoreMap> coreMapSentences = documentSentences.get(TokensAnnotation.class);

		for (CoreMap sentence : coreMapSentences) {
			edu.stanford.nlp.simple.Sentence countTokensSentence = new edu.stanford.nlp.simple.Sentence(
					sentence);
			System.out.println();
		}
	
	}
	
	
	
}
