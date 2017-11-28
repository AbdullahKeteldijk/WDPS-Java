package Spark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import net.htmlparser.jericho.Source;
import scala.Tuple2;

/**
 * This class is used to as a wrapper for the spark script to be executed. It
 * was developed during the Web Data Processing course at the VU Amsterdam.
 * 
 * @author kevin
 *
 */
public class SparkScript {
	// Define static variables
	private static JavaSparkContext context;
	private static final Logger logger = LogManager.getLogger("Extraction Logger");

	/**
	 * main method, where the execution for spark is started.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Grab the input params from the console.
		String inputdir = null;
		String useLocalMode = null;
		if (args.length == 2) {
			inputdir = args[0];
			useLocalMode = args[1];
		}
		// If one wants to run the program on a local machine for testing the has to be
		// true.
		if (useLocalMode == null)
			useLocalMode = "true";
		// If no input directory is specified, the default one is taken.
		if (inputdir == null)
			inputdir = "/home/kevin/Documents/WDPS/wdps2017/CommonCrawl-sample.warc.gz";
		useLocalMode = useLocalMode.toLowerCase();
		// Depending on the input params, set the spark context to either local or
		// cluster mode.
		SparkConf conf;
		if (useLocalMode.equals("true"))
			conf = new SparkConf().setAppName("Extraction").setMaster("local[3]");// .setJars(jars);
		else
			conf = new SparkConf().setAppName("Extraction");
		// Create a SparkContext from the previous parameters.
		context = new JavaSparkContext(conf);
		/*
		 * Since WARC archive files are not delimited by a newline, one has to use the
		 * following code to split the file by the character sequence "WARC/1.0", which
		 * indicates the beginning of a new WARC record. What happens here is the
		 * following: 1) Divide the input in single WARC records 2) Check the type of
		 * the request and just keep relevant ones. 3) Clean the HTML 4) If texts are
		 * longer than 10000 characters, split them up in multiple parts. This is
		 * necessary, because CoreNLP seems to run out of memory for very long
		 * sentences. Since the data we have is quite dirty, we can't omit these types
		 * of sentences.
		 */
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("textinputformat.record.delimiter", "WARC/1.0");
		JavaRDD<Tuple2<String, String>> rdd = context
				.newAPIHadoopFile(inputdir, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values()
				.flatMap(f -> {
					String text = ("WARC/1.0" + f.toString()).trim();
					ArrayList<CustomWarcRecord> outputList = new ArrayList<CustomWarcRecord>();
					InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8.name()));
					WarcReader reader = WarcReaderFactory.getReader(is);
					WarcRecord record;
					while ((record = reader.getNextRecord()) != null) {
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
					return outputList.iterator();
				}).flatMap(record -> {
					String recordID = record.getRecordID();
					Source source = new Source(record.getContent().toString());
					String parsedContent = source.getTextExtractor().setIncludeAttributes(false).toString();
					ArrayList<Tuple2<String, String>> splitbyLine = new ArrayList<Tuple2<String, String>>();
					int maxLenght = 10000;
					Pattern p = Pattern.compile("\\G\\s*(.{1," + maxLenght + "})(?=\\s|$)", Pattern.DOTALL);
					Matcher m = p.matcher(parsedContent);
					while (m.find())
						splitbyLine.add(new Tuple2<String, String>(recordID, m.group(1)));
					return splitbyLine.iterator();
				}).repartition(75);
		/*
		 * This part does the annotations using CoreNLP. It uses a mapPartition, so that
		 * the CoreNLP pipeline has to be initialized just as often as there are
		 * partitions and not per record, since that is quite costly. For each found
		 * entity, the programm queries the Elasticsearch instance and if an Freebase ID
		 * is found, the entity together with the ID are returned
		 */
		JavaRDD<Tuple2<String, Tuple2<String, String>>> outputRDD = rdd.mapPartitions(tuples -> {
			Properties props = new Properties();
			props.put("language", "english");
			props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
			props.setProperty("ner.useSUTime", "false");
			props.setProperty("ner.applyNumericClassifiers", "false");
			ArrayList<Tuple2<String, Tuple2<String, String>>> output = new ArrayList<Tuple2<String, Tuple2<String, String>>>();
			StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
			while (tuples.hasNext()) {
				Tuple2<String, String> tuple = tuples.next();
				String line = tuple._2;
				Annotation documentSentencesTokens = new Annotation(line);
				pipeline.annotate(documentSentencesTokens);
				List<CoreMap> coreMapSentences = documentSentencesTokens.get(SentencesAnnotation.class);
				for (CoreMap sentence : coreMapSentences) {
					List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
					for (CoreLabel t : tokens) {
						if (!t.ner().equals("O") && !t.ner().equals("TIME") && !t.ner().equals("DATE")
								&& !t.ner().equals("NUMBER")) {
							String searchTerm = t.originalText();
							URL url = new URL("http://10.149.0.127:9200/freebase/label/_search?q=" + searchTerm);
							String response = "";
							boolean foundEntry = false;
							String freebaseID = "";
							String name = "";
							try (BufferedReader reader = new BufferedReader(
									new InputStreamReader(url.openStream(), "UTF-8"))) {
								for (String respline; (respline = reader.readLine()) != null;) {
									response += respline;
								}
								JSONObject json = new JSONObject(response);
								JSONObject hitsObj = json.getJSONObject("hits");
								JSONArray hitsArr = hitsObj.getJSONArray("hits");
								JSONObject first = hitsArr.getJSONObject(0);
								JSONObject source = first.getJSONObject("_source");

								freebaseID = source.getString("resource");
								name = source.getString("label");
								foundEntry = true;
							} catch (Exception e) {
								foundEntry = false;
							}
							if (foundEntry)
								output.add(new Tuple2<String, Tuple2<String, String>>(tuple._1,
										new Tuple2<String, String>(t.originalText(), freebaseID)));
							else
								continue;
						}
					}
				}

			}
			return output.iterator();
		});
		System.out.println(outputRDD.collect());

	}
}
