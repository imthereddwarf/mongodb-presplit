package com.mongodb.preSplit;

import java.util.Calendar;

import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

public interface genChunk {
	void emitChunk(Long account,Long device,Calendar event, shard myShard);
	void emitChunk(Document key,Calendar event, shard myShard);
	void emitChunk(Long account,Long device,MinKey event, shard myShard);
	void emitChunk(Document key,MinKey event, shard myShard);
	void emitChunk(Long account,Long device,MaxKey event, shard myShard);
	void emitChunk(Document key,MaxKey event, shard myShard);
	void emitChunk(MaxKey account,shard myShard);
	void clearChunks();
}
