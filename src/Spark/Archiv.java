package Spark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

public class Archiv {
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
		JavaRDD<CustomWarcRecord> rdd = context
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
				}).repartition(75);


		System.out.println(rdd.count());

	}
}
