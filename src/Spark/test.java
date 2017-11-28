package Spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.json.JSONArray;
import org.json.JSONObject;




public class test {


	
	public static void main(String[] args) throws IOException {
		String searchTerm = "Obama";
		URL url = new URL("http://10.149.0.127:9200/freebase/label/_search?q="+searchTerm);
		String response = "";
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
		    for (String line; (line = reader.readLine()) != null;) {
		        response+=line;
		    }
		}
//		
		JSONObject json = new JSONObject(response);
		JSONObject hitsObj = json.getJSONObject("hits");
		JSONArray hitsArr = hitsObj.getJSONArray("hits");
		JSONObject first = hitsArr.getJSONObject(0); 
		JSONObject source = first.getJSONObject("_source");

		String freebaseID = source.getString("resource");
		String name = source.getString("label");

		System.out.println(name + ": " + freebaseID + "\n" );
	}
}
