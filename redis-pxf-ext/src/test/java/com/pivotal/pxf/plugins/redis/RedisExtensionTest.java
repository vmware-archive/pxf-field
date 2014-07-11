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
	}

	@After
	public void cleanup() throws Exception {
		extraParams.clear();
	}

	@Test(expected = InvocationTargetException.class)
	public void testNoHostsDefined() throws Exception {

		extraParams.add(new Pair<String, String>("HASHKEY", "test"));
		List<String> output = new ArrayList<String>();

		output.add("key1,value1");
		output.add("key2,value2");

		super.assertUnorderedOutput(new Path("/redis"), output);
	}

	@Test(expected = InvocationTargetException.class)
	public void testNoHashKeyDefined() throws Exception {
		extraParams.add(new Pair<String, String>("HOSTS", "phd2"));
		super.assertUnorderedOutput(new Path("/redis"), new ArrayList<String>());
	}

	@Test(expected = JedisConnectionException.class)
	public void testReadMultipleHostFailToConnect() throws Exception {

		extraParams.add(new Pair<String, String>("HOSTS", "phd2,phd4"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));
		super.assertUnorderedOutput(new Path("/redis"), new ArrayList<String>());
	}

	@Test(expected = JedisConnectionException.class)
	public void testReadSingleHostWrongPort() throws Exception {

		extraParams.add(new Pair<String, String>("HOSTS", "phd2:6381"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));

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

		extraParams.add(new Pair<String, String>("HOSTS", "phd2"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));

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

		extraParams.add(new Pair<String, String>("HOSTS", "phd2:6380"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));

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

		extraParams.add(new Pair<String, String>("HOSTS", "phd2,phd3"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));

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
