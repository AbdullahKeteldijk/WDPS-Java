package Spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class test {


	
	public static void main(String[] args) throws IOException {
		String searchTerm = "Obama";
		URL url = new URL("http://10.149.0.127:9200/freebase/label/_search?q="+searchTerm);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
		    for (String line; (line = reader.readLine()) != null;) {
		        System.out.println(line);
		    }
		}
	}
}
