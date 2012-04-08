package org.apache.nutch.indexer.elasticsearch;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.*;

public class ElasticsearchWriter  implements NutchIndexWriter{

	private Client client;
	private Node node;
	
	@Override
	public void open(JobConf job, String name) throws IOException {
		
		String url = job.get(ElasticsearchConstants.SERVER_URL);
		int port = Integer.parseInt(job.get(ElasticsearchConstants.SERVER_PORT));
		
		// Need to do this if the cluster name is changed, probably need to set this and sniff the cluster
		/* Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "myClusterName").build()
                .put("client.transport.sniff", true).build();*/
		
		client = new TransportClient()
        .addTransportAddress(new InetSocketTransportAddress(url, port));
		
		//node = nodeBuilder().client(true).node();
		//client = node.client();
	}

	@Override
	public void write(NutchDocument doc) throws IOException {
		
		
		
		// Set up the es index response 
		String uuid = UUID.randomUUID().toString();
		IndexRequestBuilder response = client.prepareIndex("nutch", "index", uuid);
		Map<String,Object> mp = new HashMap<String, Object>();
						
	    for(final Entry<String, NutchField> e : doc) {  
	      for (final Object val : e.getValue().getValues()) {
	    	String key;
	        // normalise the string representation for a Date
	        Object val2 = val;
	        if (val instanceof Date){
	          key = e.getKey();
	          val2 = DateUtil.getThreadLocalDateFormat().format(val);
	          mp.put(key, val2);
	        } else {
	          key = e.getKey();
	          mp.put(key, val);
	        }
	        
	      }
	    }
	    // insert the document into elasticsearch
	    response.setSource(mp);
	    response.execute();
	    
	}

	@Override
	public void close() throws IOException {
		client.close();
		//node.close();
	}

}
