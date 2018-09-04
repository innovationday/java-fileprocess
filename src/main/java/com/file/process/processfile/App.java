package com.file.process.processfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Hello world!	
 *
 */
public class App 
{
	 public static void main(String[] args) throws Exception {
		 Date stdate = new Date(); 
	        File input = new File("TradesBulk.csv");
	        File output = new File("TradesBulk.json");

	        List<Map<?, ?>> data = readObjectsFromCsv(input);
	        writeAsJson(data, output);
	        Date enddate= new Date();
	        long seconds = (enddate.getTime()-stdate.getTime())/1000;
	        System.out.println(seconds+" seconds");
	    }

	    public static List<Map<?, ?>> readObjectsFromCsv(File file) throws IOException {
	        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
	        CsvMapper csvMapper = new CsvMapper();
	        @SuppressWarnings("deprecation")
			MappingIterator<Map<?, ?>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);

	        return mappingIterator.readAll();
	    }

	    public static void writeAsJson(List<Map<?, ?>> data, File file) throws IOException {
	        ObjectMapper mapper = new ObjectMapper();
	       String jsonstr= mapper.writeValueAsString(data);
	       storeinDB(jsonstr);
	    }
	    
	    public static void storeinDB(String jsonstr){
	    	JSONArray jsonArray = new JSONArray(jsonstr);
	    	System.out.println("arr"+jsonArray.length());
	    	Mongo mongo = new Mongo("ipaddress", 27017);
            DB db = mongo.getDB("local");
            DBCollection collection = db.getCollection("test");
            List<DBObject> listObject = new ArrayList<DBObject>();     
            for(int i=0;i<jsonArray.length();i++){
            	DBObject obj = (DBObject) com.mongodb.util.JSON.parse(((JSONObject)jsonArray.get(i)).toString());
            	listObject.add(obj);
            	
            }
	    	
            
	    	//put all obj into a list,

	    	
	    	
	    	//save them into database:
	    	new MongoClient().getDB("local").getCollection("test").insert(listObject);
	    	
	    	
	    }
}
