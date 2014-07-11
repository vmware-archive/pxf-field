package com.pivotal.pxf;

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

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.accessors.IWriteAccessor;
import com.pivotal.pxf.accessors.RedisHashAccessor;
import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.RedisFragmenter;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.IWriteResolver;
import com.pivotal.pxf.resolvers.RedisHashResolver;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.PxfUnit;

public class RedisExtensionTest extends PxfUnit {

	private static List<Pair<String, Integer>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();
	private static boolean enableTests = false;
	static {

		columnDefs = new ArrayList<Pair<String, Integer>>();

		columnDefs.add(new Pair<String, Integer>("key", GPDBWritable.TEXT));
		columnDefs.add(new Pair<String, Integer>("value", GPDBWritable.TEXT));
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

	@Test
	public void testWriteSingleHost() throws Exception {
		if (!enableTests) {
			return;
		}

		extraParams.add(new Pair<String, String>("HOSTS", "phd2,phd3"));
		extraParams.add(new Pair<String, String>("HASHKEY", "test"));

		List<String> input = new ArrayList<String>();

		input.add("key1,value1");
		input.add("key2,value2");
		input.add("key3,value3");

		super.assertUnorderedInput(input, new Path("/redis"));
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
	public Class<? extends IReadAccessor> getReadAccessorClass() {
		return RedisHashAccessor.class;
	}

	@Override
	public Class<? extends IReadResolver> getReadResolverClass() {
		return RedisHashResolver.class;
	}

	@Override
	public Class<? extends IWriteAccessor> getWriteAccessorClass() {
		return RedisHashAccessor.class;
	}

	@Override
	public Class<? extends IWriteResolver> getWriteResolverClass() {
		return RedisHashResolver.class;
	}

	@Override
	public List<Pair<String, Integer>> getColumnDefinitions() {
		return columnDefs;
	}

}
