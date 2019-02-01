package com.mongodb.preSplit;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.MinKey;
import org.bson.types.MaxKey;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final MinKey minkey = new MinKey();
    private static final MaxKey maxkey = new MaxKey();
    
    /*
     *  Decision constants
     */
    private static final Long chunkFull = 17500L;
    private static final Long chunkMax = 20000L;
    private static final Long deviceHot = 20000L;
    
    
    private static String splitColl = "deviceState.deviceState";
    private static String key1 = "accountId";
    private static String key2 = "deviceId";
    private static String key3 = "EventDate";
    
    private static shard aShards[];
    private static Long docCount = 0l;
    private static Long lastReportedDocs = 0l;
    
    private static MongoCollection<Document> chunkColl = null;
    
    private static Document breakPt = new Document("accountId",minkey)
			.append("deviceId",minkey)
			.append("eventDate",minkey);
    private static String nextChunk = "deviceState.deviceState-accountId_MinKeydeviceId_MinKeyeventDate_Min";

	public static void main( String[] args )
    {
		int i;
		Long chunkEvents = 0l;
		Long deviceEvents = 0l;
		Long currentDevice = -1l;
		long currentAccount = -1l;
		
		String propsFile = "default.properties";
		
		if (args.length == 2 && args[0].equalsIgnoreCase("--config")) {
			propsFile = args[1];
		}
		// create and load default properties
		Properties defaultProps = new Properties();
		try {
			FileInputStream in = new FileInputStream(propsFile);
			defaultProps.load(in);
			in.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		}

		key1 = defaultProps.getProperty("Key1");
		key2 = defaultProps.getProperty("Key2");
		key3 = defaultProps.getProperty("Key3");
		splitColl = defaultProps.getProperty("TargetNS");

		System.out.println("Connection is: "+defaultProps.getProperty("SourceConnection"));
		MongoClientURI srcURI = new MongoClientURI(defaultProps.getProperty("SourceConnection"));
		MongoClientURI dstURI = new MongoClientURI(defaultProps.getProperty("DestinationConnection"));
		
		MongoClient sourceClient = new MongoClient(srcURI);
        MongoDatabase sourceDB = sourceClient.getDatabase(defaultProps.getProperty("SourceDatabase"));
        
        MongoClient destClient = new MongoClient(dstURI);
        MongoDatabase confDB = destClient.getDatabase("config");
        MongoDatabase destDB = destClient.getDatabase(defaultProps.getProperty("DestinationDatabase"));
        
        chunkColl = destDB.getCollection(defaultProps.getProperty("DestinationCollection"));
        /*
         *  Drop existing chunk definitions
         */
        chunkColl.deleteMany(eq("ns", splitColl));
        /*
         * Count the existing shards
         */
        MongoCollection<Document> shardColl = confDB.getCollection("shards");
        MongoCursor<Document> shardCur = shardColl.find().iterator();
        
        App.aShards = new shard[(int) shardColl.countDocuments()];
        
        i =0;
        while (shardCur.hasNext()) {
        	Document thisDoc = shardCur.next();
        	shard newShard = new shard(thisDoc.getString("_id"));
        	aShards[i++] = newShard;
        }
        
        //MongoCollection<Document> shardColl = sourceDB.getCollection("shards");
        
        MongoCollection<Document> sourceColl = sourceDB.getCollection("shardStats");
        //FindIterable<Document> documents = sourceColl.find().sort(Sorts.orderBy(Sorts.ascending("_id")))
        MongoCursor<Document> cursor = sourceColl.find().sort(Sorts.orderBy(Sorts.ascending("_id"))).noCursorTimeout(true).iterator();   
        
        LinkedHashMap<String,Long> splits = new LinkedHashMap<String,Long>();
        try {
	        while (cursor.hasNext()) {
	        	Document thisRow = cursor.next();
	        	Document group = (Document) thisRow.get("_id");
	        	long accid = (long) group.getLong("AccountID");
	        	long devid = (long) group.getLong("Device");
	        	String mon = group.getString("Month");
	        	long count = thisRow.getDouble("EventCount").longValue();
	        	if (accid == currentAccount && devid == currentDevice) {  /* Same Device */
	        		splits.put(mon, count);
	        		deviceEvents += count;
	        	}
	        	else {
	        		if ( chunkEvents + deviceEvents > chunkFull) {  /* New device so break here */
	        			chunkEvents = generateChunk(accid,devid, splits,chunkEvents, deviceEvents);
	        			splits.clear();
	        			splits.put(mon, count);
	        			deviceEvents = count;
	        		}
	        		else {   /* Multiple devices in the chunk */
	        			splits.clear();
	        			splits.put(mon, count);
	        			chunkEvents += deviceEvents;  /* Count previous device */
	        			deviceEvents = count;
	        		}
        			currentAccount = accid;
        			currentDevice = devid;
	        		
	        	}
	        }
        } catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        finally {
        
            cursor.close();
            shard lastShard = findShard(false);
            emitChunk(maxkey,lastShard);   /* Everything left goes to the end */
            lastShard.addAllocatedChunks(1L);
        }
		
		for (shard test : App.aShards) {
			System.out.println("Shard "+test.getShardName()+" "+test.getAllocatedChunks()+" chunks.");
		}
        
        		
    }
	private static Long generateChunk(Long acc, Long dev, LinkedHashMap<String,Long> perMonth,Long chunkEvents, Long noEvents) {
		
        SimpleDateFormat parser=new SimpleDateFormat("yyyyMMdd");
        docCount += chunkEvents;  /* Counts the preceding docs */
        
		if (noEvents > deviceHot) {
			shard myShard = findShard(true);
			emitChunk(acc,dev,minkey,myShard);
			Long numChunks = 1L;
			
			Set set = perMonth.entrySet();
			Iterator i = set.iterator();
			
			while (i.hasNext()) {
				@SuppressWarnings("unchecked")
				Map.Entry<String, Long> me = (Map.Entry<String,Long>)i.next();
	        	String mon = me.getKey();
	        	Long count = me.getValue();
	        	docCount += count;
	       
	        	Date monStart = null;
	        	
	        	try {
					monStart = parser.parse(mon+"01");
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	Calendar monFirst = Calendar.getInstance();
	        	monFirst.setTime(monStart);
	        	Calendar monEnd = (Calendar)monFirst.clone();
	        	monEnd.add(Calendar.MONTH, 1);
	        	if (count < chunkMax) {
	        	//	System.out.println("1 Month for: "+mon);
	        		emitChunk(acc,dev,monEnd,myShard);
	        		numChunks++;
	        	}
	        	else {
	        		Calendar monMid = (Calendar)monFirst.clone();
	        		Long chunksRequired = (count / chunkMax)+1;
	        		if (chunksRequired == 2) {     
	        		//	System.out.println("2 per Month for: "+mon);
	        			monMid.add(Calendar.DATE, 15);
	        			emitChunk(acc,dev,monMid,myShard);
	        			emitChunk(acc,dev,monEnd,myShard);
	        			numChunks += 2;
	        		} else if ( chunksRequired < 4) {
	        		//	System.out.println("Weekly for: "+mon);
	        			monMid.add(Calendar.DATE, 7);
	        			emitChunk(acc,dev,monMid,myShard);
	        			monMid.add(Calendar.DATE, 7);
	        			emitChunk(acc,dev,monMid,myShard);
	        			monMid.add(Calendar.DATE, 7);
	        			emitChunk(acc,dev,monMid,myShard);
	        			emitChunk(acc,dev,monEnd,myShard);
	        			numChunks += 4;
	        		} else {
	        		//	System.out.println("Daily for: "+mon);
	        			for (int ii =1; ii < monFirst.getActualMaximum(Calendar.DAY_OF_MONTH);ii++) {
	        				monMid.add(Calendar.DATE,1);
	        				emitChunk(acc,dev,monMid,myShard);
	        				numChunks++;
	        			}
	        		}
	        	}
			}
			emitChunk(acc,dev,maxkey,myShard);    /* Empty shard for future growth */
        	myShard.addAllocatedChunks(numChunks+1);
        	if ((docCount-lastReportedDocs)> 10000000) {
        		System.out.println("Done "+docCount.toString());
        		lastReportedDocs = docCount;
        	}
			return(0L); /* All data handled */
		} else {  /* Not hot */
			shard myShard = findShard(false);
			emitChunk(acc,dev,minkey,myShard);
			myShard.addAllocatedChunks(1L);
        	if ((docCount-lastReportedDocs)> 10000000) {
        		System.out.println("Done "+docCount.toString());
        		lastReportedDocs = docCount;
        	}
			return(noEvents);
		}
	}
	
	private static shard findShard(boolean isHot) {
		shard found = null;
		Long minEvent = -1L;
		Long minHot = -1L;
		
		for (shard test : App.aShards) {
			if (isHot) {
				if ((minHot == -1L) || (test.getHotDevices() < minHot) ) {
					minHot = test.getHotDevices();
					found = test;
				}
			}
			else {
				if ((minEvent == -1L) || (test.getAllocatedChunks() < minEvent) ) {
					minEvent = test.getAllocatedChunks();
					found = test;
				}
			}
		}
		if (isHot) found.addHotDevice(1L);
		return(found);
	}
	
	private static void emitChunk(Long account,Long device,Calendar event, shard myShard) {
		// 'deviceState.destColl-accountId_MinKeydeviceId_MinKeyeventDate_MinKey'
		Long epoch = event.getTimeInMillis();
		Date eventTS = Date.from(event.toInstant());
		
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, eventTS);
		
		//System.out.println(nextChunk);
		
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(1,0))
    			.append("lastmodEpoch", new BsonObjectId())
    			.append("ns", "deviceState.deviceState")
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName());
		
    	nextChunk = splitColl+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_new Date("+epoch.toString()+")";
		
    	try {
    		chunkColl.insertOne(docChunk);
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		
    	}
    	breakPt = endPt;
	}
	
	private static void emitChunk(Long account,Long device,MinKey event, shard myShard) {

		
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, event);
		
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(1,0))
    			.append("lastmodEpoch", new BsonObjectId())
    			.append("ns", "deviceState.deviceState")
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName());
    	
    	nextChunk = splitColl+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_MinKey";
		
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	private static void emitChunk(Long account,Long device,MaxKey event, shard myShard) {

		
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, event);
		
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(1,0))
    			.append("lastmodEpoch", new BsonObjectId())
    			.append("ns", "deviceState.deviceState")
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName());
		
    	nextChunk = splitColl+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_MaxKey";
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	private static void emitChunk(MaxKey account,shard myShard) {

		
		Document endPt = new Document(key1,maxkey)
				.append(key2, maxkey)
				.append(key3, maxkey);
		
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(1,0))
    			.append("lastmodEpoch", new BsonObjectId())
    			.append("ns", "deviceState.deviceState")
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName());
		
    	nextChunk = null;
    	chunkColl.insertOne(docChunk);
	}
}
