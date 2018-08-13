/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comp.esclient;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

/**
 *
 * @author vsundesh
 */
@PropertySource({ "classpath:application.properties" })
@Service
public class Esclient {
     
    @Autowired
    private Environment env;
    
    @Value( "${esclient.esindex}" )
    private String esindex;
    
    @Value( "${esclient.document}" )
    private String document;
    
    @Value( "${esclient.server}" )
    private String server;
    
    @Value( "${esclient.port}" )
    private String port;
    
    @Value( "${esclient.protocol}" )
    private String protocol;
    
    RestHighLevelClient client;
    
    public Esclient(){ };
    
    public Environment getEnv() {
            return env;
    }

    public void setEnv(Environment env) {
            this.env = env;
    }
    
    public RestHighLevelClient getClient() {
    	if(client==null) {
    		client=this.conn();
    	}
    	return client;
    }    
    
    public RestHighLevelClient conn() {            
        //client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    	client = new RestHighLevelClient(RestClient.builder(new HttpHost(server, 
    			new Integer(port), protocol)));
        return client;
    }
    
    public String getById(String ID) throws IOException{
        GetRequest getRequest = new GetRequest(esindex, document, ID);
        GetResponse getResponse = this.getClient().get(getRequest);
        
        return getResponse.toString();  
    }
    public String deleteById(String ID) throws IOException{
        DeleteRequest deleteRequest = new DeleteRequest(esindex, document, ID);
        DeleteResponse deleteResponse = this.getClient().delete(deleteRequest);
        
        return deleteResponse.toString();
    }
    
    public String updateById(Map<String, Object> jsonupdate, String ID)throws IOException{
        UpdateRequest updateRequest = new UpdateRequest(esindex, document, ID).doc(jsonupdate);
        UpdateResponse updateResponse = this.getClient().update(updateRequest);
        
        return updateResponse.toString();
    }
    
    public String addById(Map<String, Object> jsonadd, String ID)throws IOException{
        IndexRequest indexRequest = new IndexRequest(esindex, document,ID).source(jsonadd);
        IndexResponse indexResponse = this.getClient().index(indexRequest);
        
        return indexResponse.toString();
    }
    
    public String getAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest(esindex); 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
        searchRequest.source(searchSourceBuilder); 
        SearchResponse searchResponse = this.getClient().search(searchRequest);
        
        return searchResponse.toString();
    }
    
    public String filter(String filterKey, String filterValue, String filterAnalyzer) throws IOException{
        
        SearchRequest searchRequest = new SearchRequest(esindex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);
        
        searchSourceBuilder.query(qb).size(10);
        
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = this.getClient().search(searchRequest);
        
        return searchResponse.toString();
    }
    
    public String scroll(String filterKey, String filterValue, String filterAnalyzer, String scrollId) throws IOException{
        String res = null;
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));  
        if("na".equals(scrollId) ){
            
            SearchRequest searchRequest = new SearchRequest(esindex);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);

            searchSourceBuilder.query(qb).size(20);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = this.getClient().search(searchRequest);        
            res = searchResponse.toString();
            
        }else{
            System.out.println(scrollId);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            SearchResponse searchResponse = this.getClient().searchScroll(scrollRequest);     
            res = searchResponse.toString();
        }                
        return res;              
    }
    
    
    
    public void close() throws IOException {
        client.close();
    }

    
}
