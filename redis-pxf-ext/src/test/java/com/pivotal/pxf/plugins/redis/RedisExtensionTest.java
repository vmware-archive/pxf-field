package com.pivotal.pxf.plugins.redis;

import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;

public class RedisExtensionTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();
	private static boolean enableTests = false;
	private static String host1 = null, host2 = null, hashKey = null;
	private static int host1Port, host2Port;
	static {

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("key", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("value", DataType.TEXT));
	}

	@BeforeClass
	public static void setupClass() throws Exception {

		Configuration conf = new Configuration(false);
		conf.addResource(new FileInputStream("src/test/resources/redis.conf"));

		enableTests = conf.getBoolean("enable.tests", true);

		if (!enableTests) {
			System.out.println("Tests are disabled");
			return;
		}

		host1 = conf.get("redis.1.host");
		host1Port = Integer.parseInt(conf.get("redis.1.port"));
		host2 = conf.get("redis.2.host");
		host2Port = Integer.parseInt(conf.get("redis.2.port"));

		hashKey = conf.get("redis.hashkey");
	}

	@After
	public void cleanup() throws Exception {
		if (!enableTests) {
			return;
		}

		extraParams.clear();

		Jedis jedis = new Jedis(host1);
		jedis.connect();
		jedis.del(hashKey);
		jedis.disconnect();

		jedis = new Jedis(host2, host2Port);
		jedis.connect();
		jedis.del(hashKey);
		jedis.disconnect();
	}

	private class DummyException extends InvocationTargetException {
		private static final long serialVersionUID = 440710148083405187L;

	}

	@Test(expected = InvocationTargetException.class)
	public void testNoHostsDefined() throws Exception {
		if (!enableTests) {
			throw new DummyException();
		}

		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));
		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Test(expected = InvocationTargetException.class)
	public void testNoHashKeyDefined() throws Exception {
		if (!enableTests) {
			throw new DummyException();
		}

		extraParams.add(new Pair<String, String>("HOSTS", host1));
		super.assertUnorderedOutput(new Path("/redis"), new ArrayList<String>());
	}

	@Test(expected = JedisConnectionException.class)
	public void testReadMultipleHostFailToConnect() throws Exception {
		if (!enableTests) {
			throw new JedisConnectionException("");
		}

		extraParams.add(new Pair<String, String>("HOSTS", host1 + "," + host2));
		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));
		super.assertUnorderedOutput(new Path("/redis"), new ArrayList<String>());
	}

	@Test(expected = JedisConnectionException.class)
	public void testReadSingleHostWrongPort() throws Exception {
		if (!enableTests) {
			throw new JedisConnectionException("");
		}

		extraParams.add(new Pair<String, String>("HOSTS", host1 + ":"
				+ (host1Port + 1)));
		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));

		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Test
	public void testReadSingleHost() throws Exception {
		if (!enableTests) {
			return;
		}

		Jedis jedis = new Jedis(host1);

		jedis.connect();

		jedis.hset(hashKey, "key1", "value1");
		jedis.hset(hashKey, "key2", "value2");

		jedis.disconnect();

		extraParams.add(new Pair<String, String>("HOSTS", host1));
		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));

		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Test
	public void testReadSingleHostDifferentPort() throws Exception {
		if (!enableTests) {
			return;
		}

		Jedis jedis = new Jedis(host2, host2Port);

		jedis.connect();

		jedis.hset(hashKey, "key1", "value1");
		jedis.hset(hashKey, "key2", "value2");

		jedis.disconnect();

		extraParams.add(new Pair<String, String>("HOSTS", host2 + ":"
				+ host2Port));
		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));

		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Test
	public void testReadMultipleHost() throws Exception {
		if (!enableTests) {
			return;
		}

		Jedis jedis = new Jedis(host1);
		jedis.connect();
		jedis.hset(hashKey, "key1", "value1");
		jedis.hset(hashKey, "key2", "value2");
		jedis.disconnect();

		jedis = new Jedis(host1);
		jedis.connect();
		jedis.hset(hashKey, "key3", "value3");
		jedis.disconnect();

		extraParams.add(new Pair<String, String>("HOSTS", host1 + "," + host2
				+ ":" + host2Port));
		extraParams.add(new Pair<String, String>("HASHKEY", hashKey));

		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");
		output.add("key3,value3");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return RedisFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return RedisHashAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return RedisHashResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}

}
