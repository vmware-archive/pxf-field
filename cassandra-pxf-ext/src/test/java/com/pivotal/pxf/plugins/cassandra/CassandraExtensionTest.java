package com.pivotal.pxf.plugins.cassandra;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.PxfUnit;

public class CassandraExtensionTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	private static final String TEST_TABLE = "DEMO";

	private static boolean enableTests = true;

	@BeforeClass
	public static void setupClass() throws Exception {

		Configuration conf = new Configuration(false);
		conf.addResource(new FileInputStream(
				"src/test/resources/cassandra.conf"));

		enableTests = conf.getBoolean("enable.tests", true);

		if (!enableTests) {
			System.out.println("Tests are disabled");
			return;
		}

		String partitioner = conf.get("cassandra.partitioner");
		String address = conf.get("cassandra.address");

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("recordkey", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("name", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("password", DataType.TEXT));

		extraParams.add(new Pair<String, String>("ADDRESS", address));
		extraParams.add(new Pair<String, String>("COLUMN-FAMILY", "Users"));
		extraParams.add(new Pair<String, String>("PARTITIONER", partitioner));
	}

	@AfterClass
	public static void cleanupClass() throws Exception {

	}

	@Test
	public void testSimpleTable() throws Exception {
		if (!enableTests) {
			return;
		}

		List<String> output = new ArrayList<String>();

		output.add("1,adam,pw");
		output.add("1234,scott,tiger");

		super.assertUnorderedOutput(new Path(TEST_TABLE), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return CassandraFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return CassandraAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return CassandraResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
