package com.mongodb.preSplit;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.mongodb.client.model.Filters.eq;

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
    private static Long printEvery = 1000000L;
    
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
        String pe = defaultProps.getProperty("PrintEvery","1000");
        printEvery = Long.parseLong(pe);
        
        /*
         *  Drop existing chunk definitions
         */
        chunkColl.deleteMany(eq("ns", splitColl));
        
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
        
        MongoCollection<Document> sourceColl = sourceDB.getCollection(defaultProps.getProperty("StatsCollection"));
        //FindIterable<Document> documents = sourceColl.find().sort(Sorts.orderBy(Sorts.ascending("_id")))
        MongoCursor<Document> cursor = sourceColl.find()
        		.projection(new Document("_id",0)
        		.append(key1,1)
        		.append(key2,1)
        		.append(key3,1))
        		.sort(Sorts.orderBy(Sorts.ascending(key1,key2),Sorts.descending(key3)))
        		.noCursorTimeout(true).iterator();   
        
        Long curr1 = -1L;
        Long curr2 = -1L;
        String curr3 = "";
        Long monthCount = 0l;
        SimpleDateFormat genMon=new SimpleDateFormat("yyyyMM");
        
        LinkedList<monCount> splits = new LinkedList<monCount>();
        try {
	        while (cursor.hasNext()) {
	        	Document thisRow = cursor.next();
	        	
	        	Long in1 = thisRow.getLong(key1);
	        	Long in2 = thisRow.getLong(key2);
	        	Date in3 = thisRow.getDate(key3);
	        	if (in3 == null) continue; /* Ignore documents without an eventDate */
	        	String inMon = genMon.format(in3);

	        	if (in1.equals(curr1) && in2.equals(curr2) && inMon.contentEquals(curr3)) {
	        		monthCount++;
	        		continue;
	        	}
	        	//System.out.println(curr1.toString()+" - "+curr2.toString());
	        	Long accid = curr1;
	        	Long devid = curr2;
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

	        	if (accid.equals(currentAccount) && devid.equals(currentDevice)) {  /* Same Device */
	        		monCount split = new monCount(mon,count);
	        		splits.addFirst(split);
	        		deviceEvents += count;
	        	}
	        	else {
	        		if ( chunkEvents + deviceEvents > chunkFull) {  /* New device so break here */
	        			chunkEvents = generateChunk(currentAccount,currentDevice, splits,chunkEvents, deviceEvents);
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
	        		
	        	}
	        }
        } catch(Exception e) {
        	System.out.println(e.getMessage());
        	e.printStackTrace(System.out);
        }
        finally {        
            cursor.close();
            shard lastShard = findShard(false);
            emitChunk(maxkey,lastShard);   /* Everything left goes to the end */
        }
		
		for (shard test : App.aShards) {
			System.out.println("Shard "+test.getShardName()+" "+test.getAllocatedChunks()+" chunks.");
		}
        
        		
    }
	private static Long generateChunk(Long acc, Long dev, LinkedList<monCount> perMonth,Long chunkEvents, Long noEvents) {
		
        SimpleDateFormat parser=new SimpleDateFormat("yyyyMMdd");
        docCount += chunkEvents;  /* Counts the preceding docs */
        
		if (noEvents > deviceHot) {
			shard myShard = findShard(true);
			emitChunk(acc,dev,minkey,myShard);
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
	        		emitChunk(acc,dev,monEnd,myShard);
	        		numChunks++;
	        	}
	        	else {
	        		Calendar monMid = (Calendar)monFirst.clone();
	        		long chunksRequired = (count / chunkMax)+1;
	        		if (chunksRequired == 2L) {     
	        		//	System.out.println("2 per Month for: "+mon);
	        			monMid.add(Calendar.DATE, 15);
	        			emitChunk(acc,dev,monMid,myShard);
	        			emitChunk(acc,dev,monEnd,myShard);
	        			numChunks += 2;
	        		} else if ( chunksRequired < 4L) {
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
        	if ((docCount-lastReportedDocs)> printEvery) {
        		System.out.println("Done "+docCount.toString());
        		lastReportedDocs = docCount;
        	}
			return(0L); /* All data handled */
		} else {  /* Not hot */
			shard myShard = findShard(false);
			emitChunk(acc,dev,minkey,myShard);
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
