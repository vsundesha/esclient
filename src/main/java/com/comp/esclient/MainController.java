/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comp.esclient;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
/**
 *
 * @author vsundesh
 */
@RestController()
@RequestMapping("/")
public class MainController {
    
    @Autowired
    Esclient client;
    
    Gson gson = new Gson();
    
    @GetMapping(produces = "application/json")
    public String readAll() throws IOException {
        return client.getAll(); 
    }
    
    @GetMapping(value="/tool",produces = "application/json")
    public String getById(@RequestParam String id) {
        return client.getById(id);
    }
    
    @GetMapping(value="/filter",produces = "application/json")
    public ResponseEntity<String> filter(            
            @RequestParam String text,
            @RequestParam(required = false, defaultValue= "na") String scrollId) {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_JSON);
        responseHeaders.setAccessControlAllowOrigin("*");
        String res = client.filter("name", text, "standard");

        return new ResponseEntity<String>(res, responseHeaders, HttpStatus.CREATED);
    }
    
    @GetMapping(value="/scroll",produces = "application/json")
    public ResponseEntity<String> scroll(            
            @RequestParam String text,
            @RequestParam(required = false, defaultValue= "na") String scrollId
            ){
        HttpHeaders responseHeaders = new HttpHeaders();
		responseHeaders.setContentType(MediaType.APPLICATION_JSON);
		responseHeaders.setAccessControlAllowOrigin("*");
		String res = client.scroll("name", text, "standard", scrollId);
		 
		return new ResponseEntity<String>(res, responseHeaders, HttpStatus.CREATED);
        
    }

    
//    @GetMapping(value="/loaddata",produces = "application/json")
    public String loaddata(){
        try{
            URL url = new URL("https://dev-openebench.bsc.es/monitor/rest/aggregate");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.addRequestProperty("accept", "application/json");
            System.out.println(con.getResponseCode());

            try(BufferedReader in = new BufferedReader( new InputStreamReader(con.getInputStream()));
                    JsonParser parser = Json.createParser(in)){
                if (parser.hasNext() &&
                    parser.next() == JsonParser.Event.START_ARRAY){
                    Stream<JsonValue> stream = parser.getArrayStream();
                    stream.forEach(item->{
                        if (JsonValue.ValueType.OBJECT == item.getValueType()) {
                            String _id = "";
                            String name = "";
                            String description = "";
                            
                            try{
                                JsonObject o = item.asJsonObject();
                                
                                JsonObject object = o.getJsonArray("entities").getJsonObject(0).getJsonArray("tools").getJsonObject(0);
                                
                                if(object.containsKey("@id") && !object.isNull("@id")){
                                    _id = object.getString("@id")!=null?object.getString("@id"):"";  
                                }
                                if(object.containsKey("name") && !object.isNull("name")){
                                    name = object.getString("name")!=null?object.getString("name"):"";  
                                }
                                if(object.containsKey("description") && !object.isNull("description")){
                                    description = object.getString("description")!=null?object.getString("description"):"";  
                                }
                                                                                                                                
                                Map<String, Object> jsonMap = new HashMap<>();
                                jsonMap.put("name", name);
                                jsonMap.put("description", description);
                            	  client.addById(jsonMap, _id);
                            } catch (Exception ex) {
                                System.out.println("ERROR PARSING " + _id );
                                Logger.getLogger(MainController.class.getName()).log(Level.SEVERE, null, ex);
                            }                                                       
                        }
                    });
                }
            }
            con.disconnect();
        } catch (MalformedURLException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        return gson.toJson("data loaded");
    }
    
    //@GetMapping(value="/createindex",produces = "application/json")
    public String createIndex(){
        return gson.toJson(client.createIndex());
    }
}
