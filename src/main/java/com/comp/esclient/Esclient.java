/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comp.esclient;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

/**
 *
 * @author vsundesh
 */

@Service

public class Esclient {    

    @Value("${esclient.esindex}")
    private String esindex;
    @Value("${esclient.document}")
    private String document;
    @Value("${esclient.server}")
    private String server;
    @Value("${esclient.port}")
    private String port;
    @Value("${esclient.protocol}")
    private String protocol;
    
    private RestHighLevelClient client;
    
    private static final Logger logger = LogManager.getLogger();
    
    public Esclient(){ };
    
   
    
    public RestHighLevelClient getClient() {
    	if(client==null) {
    		client=this.conn();
    	}
    	return client;
    }    
    
    public RestHighLevelClient conn() {    
        logger.debug("hola");
        client = null;
        try{
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(server,new Integer(port), protocol)));
        }catch(NumberFormatException ex){
            System.out.println(ex);
        }
    	
        return client;
    }
    
    public String getById(String ID){
    	String res = "";
        GetRequest getRequest = new GetRequest(esindex, document, ID);
        GetResponse getResponse;
		try {
			getResponse = this.getClient().get(getRequest);
			res = getResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}
        return res;
    }
    
    public String deleteById(String ID){
    	String res = "";
        DeleteRequest deleteRequest = new DeleteRequest(esindex, document, ID);
        DeleteResponse deleteResponse;
		try {
			deleteResponse = this.getClient().delete(deleteRequest);
			res = deleteResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}
        
        return res;
    }
    
    public String updateById(Map<String, Object> jsonupdate, String ID){
    	String res = "";
        UpdateRequest updateRequest = new UpdateRequest(esindex, document, ID).doc(jsonupdate);
        UpdateResponse updateResponse;
		try {
			updateResponse = this.getClient().update(updateRequest);
			res = updateResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}
        return res;
    }
    
    public String addById(Map<String, Object> jsonadd, String ID){
    	String res = "";
    	IndexRequest indexRequest = new IndexRequest(esindex, document,ID).source(jsonadd);
        IndexResponse indexResponse;
		try {
			indexResponse = this.getClient().index(indexRequest);
			res = indexResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}
        return res;
    }
    
    public String getAll(){
    	String res = "";
        SearchRequest searchRequest = new SearchRequest(esindex); 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
        searchRequest.source(searchSourceBuilder); 
        SearchResponse searchResponse;
		try {
			searchResponse = this.getClient().search(searchRequest);
			return searchResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}
		return res;        
    }
    
    public String filter(String filterKey, String filterValue, String filterAnalyzer){
        String res = "";
        SearchRequest searchRequest = new SearchRequest(esindex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);       
        searchSourceBuilder.query(qb).size(10);        
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
		try {
			searchResponse = this.getClient().search(searchRequest);
			res = searchResponse.toString();
		} catch (IOException e) {
			System.out.println(e);
		}        
        return res;
    }
    
    public String scroll(String filterKey, String filterValue, String filterAnalyzer, String scrollId){
        String res = "";
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));  
        if("na".equals(scrollId) ){
            
            SearchRequest searchRequest = new SearchRequest(esindex);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);

            searchSourceBuilder.query(qb).size(20);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse;
			try {
				searchResponse = this.getClient().search(searchRequest);
				res = searchResponse.toString();
			} catch (IOException e) {
				System.out.println(e);
			}        
        }else{
            System.out.println(scrollId);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            SearchResponse searchResponse;
			try {
				searchResponse = this.getClient().searchScroll(scrollRequest);
				res = searchResponse.toString();
			} catch (IOException e) {
				System.out.println(e);
			}     
        }                
        return res;              
    }
    
    
    
    public void close() throws IOException {
        client.close();
    }
    
    // Creats an index "twitter" which doc "tweet" which contains 2 fields "name" and "description"
    public String createIndex(){
        String res = "";
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        String source = "{\n" +
                        "  \"settings\": {\n" +
                        "    \"analysis\": {\n" +
                        "      \"filter\": {\n" +
                        "        \"autocomplete_filter\":{\n" +
                        "          \"type\":\"edge_ngram\",\n" +
                        "          \"min_gram\":1,\n" +
                        "          \"max_gram\":20\n" +
                        "        }\n" +
                        "      }\n" +
                        "      , \"analyzer\": {\n" +
                        "        \"autocomplete\":{\n" +
                        "          \"type\":\"custom\",\n" +
                        "          \"tokenizer\" : \"standard\",\n" +
                        "          \"filter\" : [\n" +
                        "            \"lowercase\",\n" +
                        "            \"autocomplete_filter\"\n" +
                        "            ]\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"mappings\":{\n" +
                        "    \"tweet\":{\n" +
                        "      \"properties\":{\n" +
                        "        \"name\":{\n" +
                        "          \"type\" : \"text\",\n" +
                        "          \"analyzer\": \"autocomplete\"    \n" +
                        "        }\n" +
                        "      }\n" +
                        "      \n" +
                        "    }\n" +
                        "  }\n" +
                        "}";
        request.source(source, XContentType.JSON);
        
        try {
            CreateIndexResponse createIndexResponse = this.getClient().indices().create(request);
            res = "okay";
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Esclient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return res;
    }
    


    
}
