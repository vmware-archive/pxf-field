package com.pivotal.pxf.plugins.redis;

import java.util.Iterator;
import java.util.Map.Entry;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

import redis.clients.jedis.Jedis;

/**
 * This Redis accessor for PXF will read key/value pairs from one or more Redis
 * instances.
 */
public class RedisHashAccessor extends Plugin implements ReadAccessor {

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

		String host = inputData.getDataSource().replace("/", "");

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
}
