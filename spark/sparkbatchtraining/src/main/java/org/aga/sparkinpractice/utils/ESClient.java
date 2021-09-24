package org.aga.sparkinpractice.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Serializable;
import java.util.Map;

public class ESClient implements Serializable {
    RestHighLevelClient client;

    public ESClient(String url, int port) {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(url, port)) );
    }

    public void index(String indexName, Map<String,Object> doc) {
        try{
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .source(doc);
            client.index(indexRequest, RequestOptions.DEFAULT);
        }
        catch (Exception e){

        }


    }
}
