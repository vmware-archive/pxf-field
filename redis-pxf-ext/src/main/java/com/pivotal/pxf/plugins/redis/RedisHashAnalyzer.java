package com.pivotal.pxf.plugins.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.Analyzer;
import com.pivotal.pxf.api.AnalyzerStats;
import com.pivotal.pxf.api.utilities.InputData;

import redis.clients.jedis.Jedis;

public class RedisHashAnalyzer extends Analyzer {

	private Parameters params = null;

	private static final Log LOG = LogFactory.getLog(RedisHashAnalyzer.class);

	public RedisHashAnalyzer(InputData inputData) throws Exception {
		super(inputData);
		params = new Parameters(inputData);

	}

	@Override
	public AnalyzerStats getEstimatedStats(String data) throws Exception {

		long numberOfBlocks = params.getHosts().length;
		long numberOfTuples = 0L;

		for (String host : params.getHosts()) {
			Jedis jedis = new Jedis(host);
			numberOfTuples += jedis.hlen(params.getHashKey());
			jedis.disconnect();
		}

		AnalyzerStats stats = new AnalyzerStats(0L, numberOfBlocks,
				numberOfTuples);

		LOG.info(AnalyzerStats.dataToString(stats, params.getHostString()));

		return stats;
	}
}
