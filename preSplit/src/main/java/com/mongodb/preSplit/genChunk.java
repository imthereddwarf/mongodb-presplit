package com.mongodb.preSplit;

import static com.mongodb.client.model.Filters.eq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;

public class genChunk {

	private final String ns;
	private int count;
	private final ObjectId myEpoch;
	private final String key1;
	private final String key2;
	private final String key3;
	
    private static final MinKey minkey = new MinKey();
    private static final MaxKey maxkey = new MaxKey();
	
    private static MongoCollection<Document> chunkColl = null;
    
    private static Document breakPt;// = new Document("accountId",minkey)
	//		.append("deviceId",minkey)
	//		.append("eventDate",minkey);
    private static String nextChunk;  // = "deviceState.deviceState-accountId_MinKeydeviceId_MinKeyeventDate_Min";
	
	
	public genChunk(String name, ObjectId epoch, MongoCollection<Document> destination, String firstKey, String secondKey, 
			String thirdKey) {
		ns = name;
		count = 0;
		myEpoch = epoch;
		chunkColl = destination;
		key1 = firstKey;
		key2 = secondKey;
		key3 = thirdKey;
		nextChunk = name+"="+firstKey+"_MinKey"+secondKey+"_MinKey"+thirdKey+"_Min";
		breakPt = new Document(firstKey,minkey)
						.append(secondKey,minkey)
						.append(thirdKey,minkey);
	}
	
	public void emitChunk(Long account,Long device,Calendar event, shard myShard) {
		// 'deviceState.destColl-accountId_MinKeydeviceId_MinKeyeventDate_MinKey'

		Long epoch = event.getTimeInMillis();
		Instant epochNow = Instant.now();
		Date eventTS = Date.from(event.toInstant());
		
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, eventTS);
		
		//System.out.println(nextChunk);
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
  					
		
    	nextChunk = ns+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_new Date("+epoch.toString()+")";
		
    	try {
    		chunkColl.insertOne(docChunk);
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		
    	}
    	breakPt = endPt;
	}
	
	public void emitChunk(Document key,Calendar event, shard myShard) {
		// 'deviceState.destColl-accountId_MinKeydeviceId_MinKeyeventDate_MinKey'

		Long epoch = event.getTimeInMillis();
		Instant epochNow = Instant.now();
		Date eventTS = Date.from(event.toInstant());
		
		Document endPt = new Document();
		copyKey(key,endPt);
		endPt.put(key3, eventTS);
		
		//System.out.println(nextChunk);
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
  					
    	nextChunk = ns+"-"+key1+"_"
		+key.get(key1).toString()+key2+"_"+key.get(key2).toString()
		+key3+"_new Date("+epoch.toString()+")";
		
    	try {
    		chunkColl.insertOne(docChunk);
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		
    	}
    	breakPt = endPt;
	}
	
	public void emitChunk(Long account,Long device,MinKey event, shard myShard) {

		Instant epochNow = Instant.now();
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, event);
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
    	
    	nextChunk = ns+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_MinKey";
		
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	public void emitChunk(Document key,MinKey event, shard myShard) {

		Instant epochNow = Instant.now();
		Document endPt = new Document();
		copyKey(key,endPt);
		endPt.put(key3, event);
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
    	
    	nextChunk = ns+"-"+key1+"_"
		+key.get(key1).toString()+key2+"_"+key.get(key2).toString()
		+key3+"_MinKey";
		
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	
	public void emitChunk(Long account,Long device,MaxKey event, shard myShard) {

		Instant epochNow = Instant.now();
		Document endPt = new Document(key1,new BsonInt64(account))
				.append(key2, new BsonInt64(device))
				.append(key3, event);
		
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
		
    	nextChunk = ns+"-"+key1+"_"
		+account.toString()+key2+"_"+device.toString()
		+key3+"_MaxKey";
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	public void emitChunk(Document key,MaxKey event, shard myShard) {

		Instant epochNow = Instant.now();
		Document endPt = new Document();
		copyKey(key,endPt);
		endPt.put(key3, event);
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(2,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
    	
    	nextChunk = ns+"-"+key1+"_"
		+key.get(key1).toString()+key2+"_"+key.get(key2).toString()
		+key3+"_MaxKey";
		
    	chunkColl.insertOne(docChunk);
    	breakPt = endPt;
	}
	
	public void emitChunk(MaxKey account,shard myShard) {

		Instant epochNow = Instant.now();
		Document endPt = new Document(key1,maxkey)
				.append(key2, maxkey)
				.append(key3, maxkey);
		
		count++;
		List<Document> history = new ArrayList<>();
		history.add(new Document("validAfter", new BsonTimestamp((int)epochNow.getEpochSecond(),0)).append("shard", myShard.getShardName()));
    	Document docChunk = new Document("_id", nextChunk)
    			.append("lastmod", new BsonTimestamp(1,count))
    			.append("lastmodEpoch", myEpoch)
    			.append("ns", ns)
    			.append("min", breakPt)
    			.append("max", endPt)
    			.append("shard", myShard.getShardName())
    			.append("history", history);
		
    	nextChunk = null;
    	chunkColl.insertOne(docChunk);
	}
	
	public void clearChunks() {
		chunkColl.deleteMany(eq("ns", ns));
	}
	
	private void copyKey(Document in, Document out) {
		out.put(key1, in.get(key1));
		out.put(key2, in.get(key2));
		out.put(key3, in.get(key3));		
	}
}
