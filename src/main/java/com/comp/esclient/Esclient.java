/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comp.esclient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
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
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 *
 * @author vsundesh
 */
public class Esclient {
    
    private final String E_INDEX = "dev-tools";
    private final String E_TYPE = "tool";
    
    public Esclient(){};

    RestHighLevelClient client = this.conn();
    
    public RestHighLevelClient conn() {            
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        return client;
    }
    
    public String getById(String ID) throws IOException{
        GetRequest getRequest = new GetRequest(E_INDEX, E_TYPE, ID);
        GetResponse getResponse = client.get(getRequest);
        
        return getResponse.toString();  
    }
    public String deleteById(String ID) throws IOException{
        DeleteRequest deleteRequest = new DeleteRequest(E_INDEX, E_TYPE, ID);
        DeleteResponse deleteResponse = client.delete(deleteRequest);
        
        return deleteResponse.toString();
    }
    
    public String updateById(Map<String, Object> jsonupdate, String ID)throws IOException{
        UpdateRequest updateRequest = new UpdateRequest(E_INDEX, E_TYPE, ID).doc(jsonupdate);
        UpdateResponse updateResponse = client.update(updateRequest);
        
        return updateResponse.toString();
    }
    
    public String addById(Map<String, Object> jsonadd, String ID)throws IOException{
        IndexRequest indexRequest = new IndexRequest(E_INDEX, E_TYPE,ID).source(jsonadd);
        IndexResponse indexResponse = client.index(indexRequest);
        
        return indexResponse.toString();
    }
    
    public String getAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest(E_INDEX); 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
        searchRequest.source(searchSourceBuilder); 
        SearchResponse searchResponse = client.search(searchRequest);
        
        return searchResponse.toString();
    }
    
    public String filter(String filterKey, String filterValue, String filterAnalyzer) throws IOException{
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(E_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);
        
        searchSourceBuilder.query(qb).size(15);
        
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        
        return searchResponse.toString();
    }
    
    public String scroll(String filterKey, String filterValue, String filterAnalyzer, String scrollId) throws IOException{
        String res = null;
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));  
        if("na".equals(scrollId) ){
            
            SearchRequest searchRequest = new SearchRequest(E_INDEX);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder qb = QueryBuilders.matchQuery(filterKey, filterValue).analyzer(filterAnalyzer);

            searchSourceBuilder.query(qb).size(20);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest);        
           
            res = searchResponse.toString();
//            System.out.println(res);
            
        }else{
            System.out.println(scrollId);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            SearchResponse searchResponse = client.searchScroll(scrollRequest);     
            res = searchResponse.toString();
//            System.out.println(res);
        }
        
        
        return res;              
    }
    


    public void close() throws IOException {
        client.close();
    }
}
