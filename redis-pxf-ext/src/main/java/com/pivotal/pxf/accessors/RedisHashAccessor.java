package com.pivotal.pxf.accessors;

import java.util.Iterator;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.redis.Parameters;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/**
 * This Redis accessor for PXF will read key/value pairs from one or more Redis
 * instances.
 */
public class RedisHashAccessor extends Plugin implements IReadAccessor {

	private Parameters params = null;
	private Jedis readClient = null;
	// private Map<Integer, Jedis> writeClients = null;
	private Iterator<Entry<String, String>> keyvalues = null;
	private OneRow oneRow = new OneRow();
	private Entry<String, String> tmpEntry = null;

	public RedisHashAccessor(InputData inputData) throws Exception {
		super(inputData);
		params = new Parameters(inputData);
	}

	@Override
	public boolean openForRead() throws Exception {

		String host = inputData.path().replace("/", "");

		if (!host.contains(":")) {
			readClient = new Jedis(host);
		} else {
			readClient = new Jedis(host.split(":")[0], Integer.parseInt(host
					.split(":")[1]));
		}

		readClient.connect();
		keyvalues = readClient.hgetAll(params.getHashKey()).entrySet()
				.iterator();

		return true;
	}

	@Override
	public OneRow readNextObject() throws Exception {
		if (keyvalues.hasNext()) {
			tmpEntry = keyvalues.next();
			oneRow.setKey(tmpEntry.getKey());
			oneRow.setData(tmpEntry.getValue());
			return oneRow;
		} else {
			return null;
		}
	}

	@Override
	public void closeForRead() throws Exception {
		readClient.disconnect();
		readClient = null;
	}
	/*
	 * @Override public boolean openForWrite() throws Exception {
	 * 
	 * writeClients = new HashMap<Integer, Jedis>();
	 * 
	 * for (String host : params.getHosts()) { if (!host.contains(":")) {
	 * writeClients.put(writeClients.size(), new Jedis(host)); } else {
	 * writeClients.put( writeClients.size(), new Jedis(host.split(":")[0],
	 * Integer.parseInt(host .split(":")[1]))); } } return true; }
	 * 
	 * @Override public boolean writeNextObject(OneRow paramOneRow) throws
	 * Exception {
	 * 
	 * int hash = paramOneRow.getKey().hashCode() % writeClients.size();
	 * 
	 * writeClients.get(hash).hset(params.getHashKey(), (String)
	 * paramOneRow.getKey(), (String) paramOneRow.getData());
	 * 
	 * return true; }
	 * 
	 * @Override public void closeForWrite() throws Exception {
	 * readClient.disconnect(); readClient = null; }
	 */
}
