package com.mongodb.preSplit;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.mongodb.client.model.Filters.eq;

import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.BSONTimestamp;
import org.bson.types.MaxKey;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

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
    private static Long chunkFull = 17500L;
    private static Long chunkMax = 20000L;
    private static Long deviceHot = 20000L;
    
    
    private static String splitColl = "deviceState.deviceState";
    private static String key1 = "accountId";
    private static String key2 = "deviceId";
    private static String key3 = "EventDate";
    
    private static shard aShards[];
    private static Long docCount = 0l;
    private static Long lastReportedDocs = 0l;
    private static Long printEvery = 1000000L;
    

	public static void main( String[] args )
    {
		int i;
		Long chunkEvents = 0l;
		Long deviceEvents = 0l;
		Object currentDevice = null;
		Object currentAccount = null;
		Document currentKey = null;
		genChunk chunkOut = null;
		
		String propsFile = "default.properties";
		
		if (args.length == 2 && args[0].equalsIgnoreCase("--config")) {
			propsFile = args[1];
		}
		String myVersion = App.class.getPackage().getImplementationVersion();
		
		System.out.println("MongoDb Pre-split ["+myVersion+"]");
		System.out.println("Runtime -> Java: "+System.getProperty("java.vendor")+ " " + System.getProperty("java.version") + " OS: "+System.getProperty("os.name")+" " +System.getProperty("os.version"));
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
		String dbcoll[] = splitColl.split("\\.");
		String destDbName = dbcoll[0];
		String destCollName = dbcoll[1];
		
		//System.out.println("Connection is: "+defaultProps.getProperty("SourceConnection"));
		MongoClientURI srcURI = new MongoClientURI(defaultProps.getProperty("SourceConnection"));
		MongoClientURI dstURI = new MongoClientURI(defaultProps.getProperty("DestinationConnection"));
		
		MongoClient sourceClient = new MongoClient(srcURI);
        MongoDatabase sourceDB = sourceClient.getDatabase(defaultProps.getProperty("SourceDatabase"));
        
        MongoClient destClient = new MongoClient(dstURI);
        MongoDatabase confDB = destClient.getDatabase("config");
        //String destDbName = defaultProps.getProperty("DestinationDatabase");
        MongoDatabase destDB = destClient.getDatabase(destDbName);
        
        /*
         * Check the destination database is sharded and get the Primary
         */
        String primaryShard = null;
        MongoCollection<Document> dbColl = confDB.getCollection("databases");
        MongoCursor<Document> dbDef = dbColl.find(new Document("_id",destDbName)).iterator();
        if (dbDef.hasNext()) {
        	Document dbDoc = dbDef.next();
        	primaryShard = dbDoc.getString("primary");
        }
        else {
        	System.out.println(destDbName+" is not a sharded database.");
        	return;
        }
        /*
         * Check the destination collection is sharded
         */
        Document collKeys = null;
        
        MongoCollection<Document> collColl = confDB.getCollection("collections");
        MongoCursor<Document> collDef = collColl.find(new Document("_id",splitColl)).iterator();
        if (collDef.hasNext()) {
        	Document collDoc = collDef.next();
        	ObjectId epoch = collDoc.getObjectId("lastmodEpoch");
        	Date version = (Date) collDoc.get("lastmod");
			collKeys = (Document) collDoc.get("key");
        	boolean collDroped = collDoc.getBoolean("dropped");
        	if (collDroped || !collKeys.getDouble(key1).equals(1.0)) {
        		System.out.println(splitColl+" is not an active sharded collection.");
        		return;
        	}
            MongoCollection<Document> chunkColl = confDB.getCollection(defaultProps.getProperty("DestinationCollection"));
            chunkOut = new genChunk(splitColl,epoch,chunkColl,key1,key2,key3);
        }
        else {
        	System.out.println(splitColl+" is not a sharded collection.");
        	return;
        }
        
        

        
        String pe = defaultProps.getProperty("PrintEvery","1000");
        printEvery = Long.parseLong(pe);
        

        /*
         * Calculate the Maximum number of documents per chunk
         */
        
        Long blockSize = Long.parseLong(defaultProps.getProperty("ChunkSizeMb","64"))*1024L;
        Long avgDocSize = Long.parseLong(defaultProps.getProperty("DocSizekb","3"));
        
        double docsPerBlock = blockSize / avgDocSize;
        
        chunkFull = (long)(docsPerBlock * 0.8);   // Consider a chunk full at 80%
        chunkMax =  (long)(docsPerBlock * 0.9);   // Force a split based on the third level key if the same second level key accounts for > 90% of a chunk
        deviceHot = (long)(docsPerBlock * 0.9);   // If the second level key is greater then 90% of a chunk distribute away from other Hot chunks

        System.out.println("ChunkFull: "+chunkFull.toString()+ " ChunkMax: "+chunkMax.toString()+" DeviceHot: "+deviceHot.toString());
        
        //chunkFull = 2L; /* for testing */
        String shardCreds = defaultProps.getProperty("ShardCredentials");
        boolean createShards = defaultProps.getProperty("CreateSecondayShardIndex","true").toLowerCase().contentEquals("true");
        
        MongoCollection<Document> shardColl = confDB.getCollection("shards");
        MongoCursor<Document> shardCur = shardColl.find().iterator();
        App.aShards = new shard[(int) shardColl.countDocuments()];
        
        i =0;
        while (shardCur.hasNext()) {
        	Document thisDoc = shardCur.next();
        	shard newShard = new shard(thisDoc.getString("_id"),thisDoc.getString("_id").equals(primaryShard));
        	aShards[i++] = newShard;
        	/*
        	 * If not primary create the Shard Key
        	 */
        	 if (!newShard.getIsPrimary() && createShards) {
        		 String hostArr[] = thisDoc.getString("host").split("/");
        		 MongoClientURI shardURI = null;
        		 if (shardCreds == null ) 
        			 shardURI = new MongoClientURI("mongodb://"+hostArr[1]+"/?replicaSet="+hostArr[0]);
        		 else
        			 shardURI = new MongoClientURI("mongodb://"+shardCreds+"@"+hostArr[1]+"/?replicaSet="+hostArr[0]);
	        	 MongoClient shardClient = new MongoClient(shardURI);
	        	 MongoDatabase database = shardClient.getDatabase(destDbName);
	        	 MongoCollection<Document> collection = database.getCollection(destCollName);
	        	 collection.createIndex(collKeys);
	        	 shardClient.close();
        	 }
        	 
        }
        
        
        /*
         *  Drop existing chunk definitions
         */
        chunkOut.clearChunks();
        

        
        //MongoCollection<Document> shardColl = sourceDB.getCollection("shards");
        
        MongoCollection<Document> sourceColl = sourceDB.getCollection(defaultProps.getProperty("StatsCollection"));
        //FindIterable<Document> documents = sourceColl.find().sort(Sorts.orderBy(Sorts.ascending("_id")))
        MongoCursor<Document> cursor = sourceColl.find()
        		.projection(new Document("_id",0)
        		.append(key1,1)
        		.append(key2,1)
        		.append(key3,1))
        		.sort(Sorts.orderBy(Sorts.ascending(key1,key2),Sorts.descending(key3)))
        		.noCursorTimeout(true).iterator();   
        
        Object curr1 = null;
        Object curr2 = null;
        String curr3 = "";
        Long monthCount = 0l;
        SimpleDateFormat genMon=new SimpleDateFormat("yyyyMM");
        
        LinkedList<monCount> splits = new LinkedList<monCount>();
        Document thisRow = null;
        try {
	        while (cursor.hasNext()) {
	        	thisRow = cursor.next();
	        	//System.out.println(thisRow.toJson());
	        	Object in1, in2;
	        	Date in3;
	        	try {
	        		in1 = thisRow.get(key1);
	        		in2 = thisRow.get(key2);
	        		in3 = thisRow.getDate(key3);
	        		
	        	} catch (Exception e) {
	        		String message = e.getMessage();
	        		if (message == null)
	        			message = e.toString();
	        		System.out.println("Bad Key: "+thisRow.toJson()+" "+message);
	        		chunkEvents++;  // Skip but still counts
	        		continue; /* Unexpected key value - just skip */
	        	}
	        	if (in1 == null || in2 == null || in3 == null) {
	        		chunkEvents++;
	        		continue; /* Ignore documents without an full shard Key */
	        	}
	        	String inMon = genMon.format(in3);

	        	if ((in1.equals(curr1) || numeric.equals(in1, curr1)) && (in2.equals(curr2) || numeric.equals(in2, curr2)) && inMon.contentEquals(curr3)) {
	        		monthCount++;
	        		continue;
	        	}
	        	//System.out.println(curr1.toString()+" - "+curr2.toString());
	        	Object accid = curr1;
	        	Object devid = curr2;
	        	Document fullKey = cloneKey(thisRow);
	        	String mon = curr3;
	        	Long count = monthCount;
        		curr1 = in1;
        		curr2 = in2;
        		curr3 = inMon;

        		
	        	if (monthCount.equals(0L)) { /* First time through no data to record */
	        		monthCount = 1L;
	        		continue;
	        	}
        		monthCount=1L;   /* Count the read ahead entry */
	        	/*
	        	 * Finished counting a month
	        	 */

	        	if ((accid.equals(currentAccount) || numeric.equals(accid,currentAccount)) && 
	        			(devid.equals(currentDevice) || numeric.equals(devid, currentDevice))) {  /* Same Device */
	        		monCount split = new monCount(mon,count);
	        		splits.addFirst(split);
	        		deviceEvents += count;
	        	}
	        	else {
	        		if (currentAccount == null) {   // Set this doc as current if first time through
	        			currentAccount = accid;
	        			currentDevice = devid;
	        			currentKey = cloneKey(fullKey);
	        		}
	        		if ( chunkEvents + deviceEvents > chunkFull) {  /* New device so break here */
	        			chunkEvents = generateChunk(currentAccount,currentDevice, currentKey, splits,chunkEvents, deviceEvents, chunkOut);
	        			splits.clear();
		        		monCount split = new monCount(mon,count);
		        		splits.addFirst(split);
	        			deviceEvents = count;
	        		}
	        		else {   /* Multiple devices in the chunk */
	        			splits.clear();
		        		monCount split = new monCount(mon,count);
		        		splits.addFirst(split);
	        			chunkEvents += deviceEvents;  /* Count previous device */
	        			deviceEvents = count;
	        		}
        			currentAccount = accid;
        			currentDevice = devid;
        			currentKey = cloneKey(fullKey);
	        		
	        	}
	        }
        } catch(Exception e) {
        	System.out.println(e.getMessage());
        	System.out.println(thisRow.toJson());
        	e.printStackTrace(System.out);
        }
        finally {        
            cursor.close();
            shard lastShard = findShard(false);
            chunkOut.emitChunk(maxkey,lastShard);   /* Everything left goes to the end */
        }
		
		for (shard test : App.aShards) {
			System.out.println("Shard "+test.getShardName()+" "+test.getAllocatedChunks()+" chunks.");
		}
		
		MongoDatabase adminDB = destClient.getDatabase("admin");
		Document result = adminDB.runCommand(new Document("flushRouterConfig","1"));
		System.out.println("Router Cache Flushed");
        		
    }
	private static Long generateChunk(Object acc, Object dev, Document key, LinkedList<monCount> perMonth,Long chunkEvents, Long noEvents, genChunk chunkOut) {
		
        SimpleDateFormat parser=new SimpleDateFormat("yyyyMMdd");
        docCount += chunkEvents;  /* Counts the preceding docs */
        
		if (noEvents > deviceHot) {
			shard myShard = findShard(true);
			chunkOut.emitChunk(key,minkey,myShard);
			Long numChunks = 1L;
			
			ListIterator<monCount> i = perMonth.listIterator();
/* 
 * The entry date is backwards in the index so reverse the months
 */
			
			while (i.hasNext()) {
				@SuppressWarnings("unchecked")
				monCount me = (monCount)i.next();
	        	String mon = me.getMonth();
	        	Long count = me.getCount();
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
	        		chunkOut.emitChunk(key,monEnd,myShard);
	        		numChunks++;
	        	}
	        	else {
	        		Calendar monMid = (Calendar)monFirst.clone();
	        		long chunksRequired = (count / chunkMax)+1;
	        		if (chunksRequired == 2L) {     
	        		//	System.out.println("2 per Month for: "+mon);
	        			monMid.add(Calendar.DATE, 15);
	        			chunkOut.emitChunk(key,monMid,myShard);
	        			chunkOut.emitChunk(key,monEnd,myShard);
	        			numChunks += 2;
	        		} else if ( chunksRequired < 4L) {
	        		//	System.out.println("Weekly for: "+mon);
	        			monMid.add(Calendar.DATE, 7);
	        			chunkOut.emitChunk(key,monMid,myShard);
	        			monMid.add(Calendar.DATE, 7);
	        			chunkOut.emitChunk(key,monMid,myShard);
	        			monMid.add(Calendar.DATE, 7);
	        			chunkOut.emitChunk(key,monMid,myShard);
	        			chunkOut.emitChunk(key,monEnd,myShard);
	        			numChunks += 4;
	        		} else {
	        		//	System.out.println("Daily for: "+mon);
	        			for (int ii =1; ii < monFirst.getActualMaximum(Calendar.DAY_OF_MONTH);ii++) {
	        				monMid.add(Calendar.DATE,1);
	        				chunkOut.emitChunk(key,monMid,myShard);
	        				numChunks++;
	        			}
	        		}
	        	}
			}
			chunkOut.emitChunk(key,maxkey,myShard);    /* Empty shard for future growth */
        	myShard.addAllocatedChunks(numChunks+1);
        	if ((docCount-lastReportedDocs)> printEvery) {
        		System.out.println("Done "+docCount.toString());
        		lastReportedDocs = docCount;
        	}
			return(0L); /* All data handled */
		} else {  /* Not hot */
			shard myShard = findShard(false);
			chunkOut.emitChunk(key,minkey,myShard);
			myShard.addAllocatedChunks(1L);
        	if ((docCount-lastReportedDocs)> printEvery) {
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
				if (minHot.equals(-1L) || (test.getHotDevices() < minHot) ) {
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
	
	private static Document cloneKey(Document row) {
		Document key = new Document(key1,row.get(key1))
				.append(key2, row.get(key2))
				.append(key3, row.get(key3));
		return(key);
		
	}
	

}
