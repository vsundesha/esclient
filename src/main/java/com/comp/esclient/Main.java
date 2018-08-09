/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comp.esclient;  

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @author vsundesh
 */
@SpringBootApplication
public class Main{
    
    public static void main(String[] args) {        
        SpringApplication.run(Main.class, args); 
    }
}
   
    
    
    ////Add
//    ArrayList<HashMap> list = new ArrayList();
//    
//    Map<String, Object> jsonMap = new HashMap<>();
//    jsonMap.put("title", "star wars");
//    jsonMap.put("message", "trying out Elasticsearch");
//    list.add((HashMap) jsonMap);
//    
//    Map<String, Object> jsonMap1 = new HashMap<>();
//    jsonMap1.put("title", "star trek");
//    jsonMap1.put("message", "trying out Elasticsearch");
//    list.add((HashMap) jsonMap1);
//    
//    Map<String, Object> jsonMap2 = new HashMap<>();
//    jsonMap2.put("title", "starter pack");
//    jsonMap2.put("message", "trying out Elasticsearch");
//    list.add((HashMap) jsonMap2);
//    
//    Map<String, Object> jsonMap3 = new HashMap<>();
//    jsonMap3.put("title", "rastarfas");
//    jsonMap3.put("message", "trying out Elasticsearch");
//    list.add((HashMap) jsonMap3);
//    
//    Map<String, Object> jsonMap4 = new HashMap<>();
//    jsonMap4.put("title", "Nothing to do with the resta ");
//    jsonMap4.put("message", "trying out Elasticsearch");
//    list.add((HashMap) jsonMap4);
//    
//    for (int i = 0; i<list.size(); i++) {
//        IndexRequest indexRequest;
//        indexRequest = new IndexRequest(E_INDEX, E_TYPE,String.valueOf(i)).source(list.get(i));
//        IndexResponse indexResponse = client.index(indexRequest);
//    }
    
//    
//    ////Get
//    GetRequest getRequest = new GetRequest(E_INDEX, E_TYPE, "1");
//    GetResponse getResponse = client.get(getRequest);
//        System.out.println(getResponse.getSource());
//
//    ////delete
//    DeleteRequest deleteRequest = new DeleteRequest(E_INDEX, E_TYPE, "1");
//    DeleteResponse deleteResponse = client.delete(deleteRequest);
//        System.out.println(deleteResponse);
//
//    ////update
//    Map<String, Object> jsonupdate = new HashMap<>();
//    jsonupdate.put("reason", "daily updsssate");
//    UpdateRequest updateRequest = new UpdateRequest(E_INDEX, E_TYPE, "55r5kmQB48nZh1fHXAUA").doc(jsonupdate);
//    UpdateResponse updateResponse = client.update(updateRequest);
//        System.out.println(updateResponse);
//
//    ////multi Get
//    MultiGetRequest multiGetRequest = new MultiGetRequest();
//    multiGetRequest.add(new MultiGetRequest.Item(E_INDEX, E_TYPE, "1"));  
//    multiGetRequest.add(new MultiGetRequest.Item(E_INDEX, E_TYPE, "55r5kmQB48nZh1fHXAUA")); 
//    MultiGetResponse multiGetResponse = client.multiGet(multiGetRequest);
//    MultiGetItemResponse item = multiGetResponse.getResponses()[0];
//        System.out.println(item.getResponse());
//
//    //Search
//    SearchRequest searchRequest = new SearchRequest("movies");
//    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//    QueryBuilder qb = QueryBuilders.matchQuery("title", "wars").analyzer("standard");
//    searchSourceBuilder.query(qb);
//      
//    searchRequest.source(searchSourceBuilder);
//    
//    SearchResponse searchResponse = client.conn().search(searchRequest);
//    System.out.println(searchResponse);
//    
    
    
    
    
    

    
    

