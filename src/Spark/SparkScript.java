package Spark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
	public static final StanfordCoreNLP pipeline = SparkScript.StanfordDepNNParser();
	private static JavaSparkContext context;
	private static final Logger logger = LogManager.getLogger("Extraction Logger");

	public static void main(String[] args) {
		String inputdir = null;
		String useLocalMode = null;
		if (args.length == 2) {
			inputdir = args[0];
			useLocalMode = args[1];
		}

		if (useLocalMode == null)
			useLocalMode = "true";
		if (inputdir == null)
			inputdir = "/home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz";
		useLocalMode = useLocalMode.toLowerCase();
		
		System.out.println(inputdir);
		System.out.println(useLocalMode);
		
		

		// Depending on the input params, set the spark context to either local or
		// cluster mode.
		SparkConf conf;
		if (useLocalMode.equals("true"))
			conf = new SparkConf().setAppName("Extraction").setMaster("local[3]");// .setJars(jars);
		else
			conf = new SparkConf().setAppName("Extraction");
		context = new JavaSparkContext(conf);

		Configuration hadoopConf = new Configuration();
		hadoopConf.set("textinputformat.record.delimiter", "WARC/1.0");
		JavaRDD<String> rddWARC = context
				.newAPIHadoopFile(inputdir,
						TextInputFormat.class, LongWritable.class, Text.class, hadoopConf)
				.values().map(new Function<Text, String>() {
					@Override
					public String call(Text arg0) throws Exception {
						return ("WARC/1.0" + arg0.toString()).trim();
					}
				}).repartition(50);

		/// home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz
		// hdfs:///user/bbkruit/CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.gz

		JavaRDD<CustomWarcRecord> fileContentRDD = rddWARC.flatMap(fileNameContent -> {

			InputStream is = new ByteArrayInputStream(fileNameContent.getBytes(StandardCharsets.UTF_8.name()));
			ArrayList<CustomWarcRecord> outputList = new ArrayList<CustomWarcRecord>();
			if (fileNameContent.equals("WARC/1.0"))
				return outputList.iterator();

			int numRecords = 0;
			int errors = 0;
			WarcReader reader = WarcReaderFactory.getReader(is);
			WarcRecord record;
			while ((record = reader.getNextRecord()) != null) {
				if ((++numRecords) % 1000 == 0)
					System.out.println(numRecords);
				String recordId = record.getHeader("WARC-Record-ID").value;
				String contentType = record.getHeader("Content-Type").value;

				if (!contentType.equals("application/http; msgtype=response"))
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
		

		JavaRDD<AnnotatedRecord> annotatedRDD = fileContentRDD.map(record -> {
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
					if (!t.ner().equals("O") && !t.ner().equals("TIME") && !t.ner().equals("DATE")
							&& !t.ner().equals("NUMBER")) {
						Token tempToken = new Token(t.originalText(), t.ner(), t.lemma());
						tokensList.add(tempToken);
					}
				}
			}
			AnnotatedRecord output = new AnnotatedRecord(recordID, tokensList);
			return output;
		});

		JavaRDD<String> outputRDD = annotatedRDD.flatMap(record -> {
			ArrayList<String> output = new ArrayList<String>();
			for (Token t : record.getTokens()) {
				output.add("" + record.getRecordID() + "\t" + t.getText() + "\t" + t.getNer() + "\n");
			}
			return output.iterator();
		});

		System.out.println(outputRDD.collect());

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
