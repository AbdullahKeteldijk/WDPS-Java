package Spark;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
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

public class SparkScript {
	public static final StanfordCoreNLP pipeline = sd.StanfordDepNNParser();
	private static JavaSparkContext context;

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

		// conf.newAPIHadoopFile("",
		// "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
		// "org.apache.hadoop.io.LongWritable",
		// "org.apache.hadoop.io.Text",
		// conf={"textinputformat.record.delimiter": "WARC/1.0"})

		Configuration hadoopConf = new Configuration();

		// pipe character | is the record seperator
		hadoopConf.set("textinputformat.record.delimiter", "WARC/1.0");

		JavaPairRDD<String, PortableDataStream> compressedFilesRDD = context
				.binaryFiles("hdfs:///user/bbkruit/CommonCrawl-sample.warc.gz");

		JavaRDD<CustomWarcRecord> fileContentRDD = compressedFilesRDD.flatMap(fileNameContent -> {
			PortableDataStream content = fileNameContent._2();
			InputStream is = content.open();
			WarcReader warcReader = new WarcReader(is);
			WarcRecord record;
			ArrayList<CustomWarcRecord> outputList = new ArrayList<CustomWarcRecord>();
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
							CustomWarcRecord temp = new CustomWarcRecord(recordID, sb.toString());
							outputList.add(temp);
						} catch (Exception e) {
							e.printStackTrace();

						}
					}
				}
			}
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
