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
import scala.collection.Iterator;

public class SparkScript {
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
		JavaRDD<String> rdd = context
				.newAPIHadoopFile(inputdir, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values()
				.map(f -> {
					String text = ("WARC/1.0" + f.toString()).trim();
					return text;
				}).repartition(200);

		JavaRDD<AnnotatedRecord> rddWARC = rdd.mapPartitions(f -> {
			ArrayList<CustomWarcRecord> outputList = new ArrayList<CustomWarcRecord>();
			ArrayList<AnnotatedRecord> output = new ArrayList<AnnotatedRecord>();

			Properties props = new Properties();
			props.put("language", "english");
			props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
			props.setProperty("ner.useSUTime", "false");
			props.setProperty("ner.applyNumericClassifiers", "false");
			StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

			while (f.hasNext()) {
				String text = ("WARC/1.0" + f.next()).trim();
				InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8.name()));

				int numRecords = 0;
				WarcReader reader = WarcReaderFactory.getReader(is);
				WarcRecord record;
				while ((record = reader.getNextRecord()) != null) {
					if ((++numRecords) % 1000 == 0)
						System.out.println(numRecords);
					if (record.getHeader("WARC-Record-ID") == null)
						continue;
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
			}

			for (CustomWarcRecord record : outputList) {
				String recordID = record.getRecordID();
				String parsedContent =  record.getContent().toString().replaceAll("\\<.*?>"," ");
//				String parsedContent = Jsoup.parse(record.getContent()).text();
				ArrayList<Token> tokensList = new ArrayList<Token>();
				
				
				Annotation documentSentencesTokens = new Annotation(record.getContent());
				pipeline.annotate(documentSentencesTokens);
				List<CoreMap>coreMapSentences = documentSentencesTokens.get(SentencesAnnotation.class);
				for (CoreMap sentence : coreMapSentences) {
					List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
					for (CoreLabel t : tokens) {
						if (!t.ner().equals("O") && !t.ner().equals("TIME") && !t.ner().equals("DATE")
								&& !t.ner().equals("NUMBER")) {
							Token tempToken = new Token(t.originalText(), t.ner(), t.lemma());
							tokensList.add(tempToken);
						}
					}
				}
				AnnotatedRecord anRecord = new AnnotatedRecord(recordID, tokensList);
				output.add(anRecord);
			}
			return output.iterator();
		});

		/// home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz
		// hdfs:///user/bbkruit/CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.gz

		JavaRDD<String> outputRDD = rddWARC.flatMap(record -> {
			ArrayList<String> output = new ArrayList<String>();
			for (Token t : record.getTokens()) {
				output.add("" + record.getRecordID() + "\t" + t.getText() + "\t" + t.getNer() + "\n");
			}
			return output.iterator();
		});

		System.out.println(outputRDD.collect());

	}

}
