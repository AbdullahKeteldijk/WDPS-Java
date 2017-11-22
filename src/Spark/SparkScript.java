package Spark;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.jsoup.Jsoup;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


public class SparkScript {
	public static final StanfordCoreNLP pipeline = sd.StanfordDepNNParser();
	private static JavaSparkContext context;
	private static final Logger logger = LogManager.getLogger("Extraction Logger");

	public static void main(String[] args) {
		String useLocalMode = "true";

		useLocalMode = useLocalMode.toLowerCase();

		// Depending on the input params, set the spark context to either local or
		// cluster mode.
		SparkConf conf;
		if (useLocalMode.equals("true"))
			conf = new SparkConf().setAppName("Extraction").setMaster("local[3]");// .setJars(jars);
		else
			conf = new SparkConf().setAppName("Extraction");
		context = new JavaSparkContext(conf);

		Configuration hadoopConf = new Configuration();

		// pipe character | is the record seperator
		hadoopConf.set("textinputformat.record.delimiter", "WARC/1.0");
		///home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz
		//hdfs:///user/bbkruit/CommonCrawl-sample.warc.gz
		JavaPairRDD<String, PortableDataStream> compressedFilesRDD = context
				.binaryFiles("hdfs:///user/bbkruit/CommonCrawl-sample.warc.gz");

		
		JavaRDD<CustomWarcRecord> fileContentRDD = compressedFilesRDD.flatMap(fileNameContent -> {
			PortableDataStream content = fileNameContent._2();
			InputStream is = new GZIPInputStream(content.open());
			ArrayList<CustomWarcRecord> outputList = new ArrayList<CustomWarcRecord>();
			
			int numRecords = 0;
			int errors = 0;
			WarcReader reader = WarcReaderFactory.getReader(is);
			WarcRecord record;
			while ( (record = reader.getNextRecord()) != null ) {
				if ((++numRecords) % 1000 == 0)
					System.out.println(numRecords);
				String recordId = record.getHeader("WARC-Record-ID").value;
				String contentType = record.getHeader("Content-Type").value;
				
				if(!contentType.equals("application/http; msgtype=response"))
					continue;
				BufferedReader br = null;
				StringBuilder sb = new StringBuilder();

				String line;
				try {
					br = new BufferedReader(new InputStreamReader(record.getPayload().getInputStream()));
					while ((line = br.readLine()) != null) {
						sb.append(line);
					}
					CustomWarcRecord temp = new CustomWarcRecord(recordId, sb.toString());
					outputList.add(temp);
				} catch (Exception e) {
					e.printStackTrace();

				}
			}

			reader.close();
			is.close();

			return outputList.iterator();
		});
		
		JavaRDD<AnnotatedRecord> annotatedRDD = fileContentRDD.map(record-> {
			String recordID = record.getRecordID();
			String parsedContent = Jsoup.parse(record.getContent()).text();
			
			Annotation documentSentences = new Annotation(parsedContent);
			pipeline.annotate(documentSentences);

			List<CoreMap> coreMapSentences = documentSentences.get(SentencesAnnotation.class);
			ArrayList<Token> tokensList = new ArrayList<Token>();
			for (CoreMap sentence : coreMapSentences) {
				// Get the tokens
				List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
				for (CoreLabel t : tokens) {
					if (!t.ner().equals("O")&&!t.ner().equals("TIME")&&!t.ner().equals("DATE")&&!t.ner().equals("NUMBER")) {
						Token tempToken = new Token(t.originalText(),t.ner(), t.lemma());
						tokensList.add(tempToken);
					}
				}
			}
			AnnotatedRecord output = new AnnotatedRecord(recordID, tokensList);
			return output;
		});
		
		JavaRDD<String> outputRDD = annotatedRDD.flatMap(record -> {
			ArrayList<String> output = new ArrayList<String>();
			for(Token t: record.getTokens()) {
				output.add(""+record.getRecordID()+"\t"+t.getText()+"\t"+t.getNer()+"\n");
			}
			return output.iterator();
		});

		System.out.println(outputRDD.collect());

	}

}
