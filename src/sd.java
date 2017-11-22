import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.jsoup.Jsoup;

import com.mixnode.warcreader.WarcReader;
import com.mixnode.warcreader.record.WarcRecord;
import com.mixnode.warcreader.record.WarcRecord.WarcType;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class sd {
	public static final StanfordCoreNLP pipeline = sd.StanfordDepNNParser();

	public static void main(String[] args) {

		InputStream initialStream = null;
		try {
			initialStream = new FileInputStream(
					new File("/home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz"));
			WarcReader warcReader = new WarcReader(initialStream);
			WarcRecord record;
			int numRecords = 0;
			int i = 0;
			while ((record = warcReader.readRecord()) != null) {
				if ((++numRecords) % 1000 == 0)
					System.out.println(numRecords);
				if (record.getType() == WarcType.response) {
					HttpResponse resp = (HttpResponse) record.getWarcContentBlock();
					String recordID = record.getWarcHeaders().getFirstHeader("WARC-Record-ID").getValue();
					if (resp.containsHeader("Content-Type") == false) {
						System.out.println("No content type");
					} else {
						BufferedReader br = null;
						StringBuilder sb = new StringBuilder();

						String line;
						try {
							br = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
							while ((line = br.readLine()) != null) {
								sb.append(line);
							}
							String x = html2text(sb.toString());
							Annotation documentSentences = new Annotation(x);
							pipeline.annotate(documentSentences);

							List<CoreMap> coreMapSentences = documentSentences.get(SentencesAnnotation.class);

							for (CoreMap sentence : coreMapSentences) {
								// Get the tokens
								List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
								for (CoreLabel t : tokens) {
									if (!t.ner().equals("O")&&!t.ner().equals("TIME")&&!t.ner().equals("DATE")&&!t.ner().equals("NUMBER")) {
										System.out.print(recordID);
										System.out.print(t.word() + "\t");
										System.out.println(t.ner());
									}
								}
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static String html2text(String html) {
		return Jsoup.parse(html).text();
	}

	public static StanfordCoreNLP StanfordDepNNParser() {
		Properties props = new Properties();

		props.put("language", "english");
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
		props.put("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_SD.gz");
		props.put("parse.originalDependencies", true);
		props.setProperty("ner.useSUTime", "false");
		props.setProperty("coref.algorithm", "statistical");
		props.setProperty("coref.maxMentionDistance", "30"); // default = 50
		props.setProperty("coref.maxMentionDistanceWithStringMatch", "250"); // default = 500
		// Probably not needed, since we don't train a new model.
		// But if, for some reason, a new model is trained this will reduce the memory
		// load in the training
		props.setProperty("coref.statistical.maxTrainExamplesPerDocument", "1100"); // Use this to downsample examples
																					// from larger documents. A value
																					// larger than 1000 is recommended.
		props.setProperty("coref.statistical.minClassImbalance", "0.04"); // A value less than 0.05 is recommended.

		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		return pipeline;
	}
}
