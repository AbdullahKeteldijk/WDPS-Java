package Spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class test {


	
	public static void main(String[] args) throws IOException {
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", "web-data-processing-systems").build();
		
		Client client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("http://10.149.0.127"), 9200));

		// on shutdown
		
		
		SearchResponse response = client.prepareSearch("freebase")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("multi", "obama"))                 // Query
		        .setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();
		
		for (SearchHit sh: response.getHits()){
			System.out.println(sh.toString());
		}
		client.close();
	}
}
