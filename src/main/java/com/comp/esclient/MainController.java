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
    
    Esclient client = new Esclient();
    Gson gson = new Gson();
    
    @GetMapping(produces = "application/json")
    public String readAll() {
        try {
            return client.getAll();
        } catch (IOException ex) {
            Logger.getLogger(MainController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    @GetMapping(value="/tool",produces = "application/json")
    public String getById(@RequestParam String id) {
        try {
            return client.getById(id);
        } catch (IOException ex) {
            Logger.getLogger(MainController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    @GetMapping(value="/filter",produces = "application/json")
    public ResponseEntity<String> filter(            
            @RequestParam String text,
            @RequestParam(required = false, defaultValue= "na") String scrollId
            ){
        try {
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setContentType(MediaType.APPLICATION_JSON);
            responseHeaders.setAccessControlAllowOrigin("*");
            String res = client.filter("name", text, "standard");
            
            return new ResponseEntity<String>(res, responseHeaders, HttpStatus.CREATED);
            
        } catch (IOException ex) {
            Logger.getLogger(MainController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    @GetMapping(value="/scroll",produces = "application/json")
    public ResponseEntity<String> scroll(            
            @RequestParam String text,
            @RequestParam(required = false, defaultValue= "na") String scrollId
            ){
        try {
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setContentType(MediaType.APPLICATION_JSON);
            responseHeaders.setAccessControlAllowOrigin("*");
            String res = client.scroll("name", text, "standard", scrollId);
            
            return new ResponseEntity<String>(res, responseHeaders, HttpStatus.CREATED);
            
        } catch (IOException ex) {
            Logger.getLogger(MainController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    
    @GetMapping(value="/loaddata",produces = "application/json")
    public String loaddata(){
        try{
            URL url = new URL("https://openebench.bsc.es/monitor/tool");
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
                            String version = "";
                            
                            try{
                                JsonObject object = item.asJsonObject();
                                _id  = object.getString("@id");
                                
                                if(object.containsKey("name") && !object.isNull("name")){
                                    name = object.getString("name")!=null?object.getString("name"):"";  
                                }
                                if(object.containsKey("description") && !object.isNull("description")){
                                    description = object.getString("description")!=null?object.getString("description"):"";  
                                }
                                if(object.containsKey("@version") && !object.isNull("@version")){
                                    version = object.getString("@version")!=null?object.getString("@version"):"";         
                                }
                                                                                                                                
                                Map<String, Object> jsonMap = new HashMap<>();
                                jsonMap.put("name", name);
                                jsonMap.put("description", description);
                                jsonMap.put("version", version);
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
    

}