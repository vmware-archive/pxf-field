package com.pivotal.pxf.analyzers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import com.pivotal.pxf.redis.Parameters;
import com.pivotal.pxf.utilities.InputData;

public class RedisHashAnalyzer extends Analyzer {

	private Parameters params = null;

	private static final Log LOG = LogFactory.getLog(RedisHashAnalyzer.class);

	public RedisHashAnalyzer(InputData inputData) throws Exception {
		super(inputData);
		params = new Parameters(inputData);

	}

	@Override
	public DataSourceStatsInfo GetEstimatedStats(String data) throws Exception {

		long numberOfBlocks = params.getHosts().length;
		long numberOfTuples = 0L;

		for (String host : params.getHosts()) {
			Jedis jedis = new Jedis(host);
			numberOfTuples += jedis.hlen(params.getHashKey());
			jedis.disconnect();
		}

		DataSourceStatsInfo stats = new DataSourceStatsInfo(0L, numberOfBlocks,
				numberOfTuples);

		LOG.info(DataSourceStatsInfo.dataToString(stats, params.getHostString()));

		return stats;
	}
}
