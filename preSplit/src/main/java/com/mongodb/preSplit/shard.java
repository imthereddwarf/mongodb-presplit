package com.mongodb.preSplit;

public class shard {

	private final String shardName;
	private Long allocatedChunks;
	private Long hotDevices;
	

	public void addAllocatedChunks(Long allocatedChunks) {
		this.allocatedChunks += allocatedChunks;
	}

	public void addHotDevice(Long hotDevices) {
		this.hotDevices += hotDevices;
	}

	public shard(String name) {
		shardName = name;
		allocatedChunks = 0L;
		hotDevices = 0L;
	}

	public String getShardName() {
		return shardName;
	}

	public Long getAllocatedChunks() {
		return allocatedChunks;
	}

	public Long getHotDevices() {
		return hotDevices;
	}
	
}
