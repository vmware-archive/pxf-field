package com.pivotal.pxf.plugins.accumulo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;

public class AccumuloExtensionTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();
	private static Connector conn = null;
	private static final String EMPTY_TABLE = "pxf_accumulo_ext_emptytable";
	private static final String DNE_TABLE = "pxf_accumulo_ext_notatable";
	private static final String TEST_TABLE = "pxf_accumulo_ext_testtable";
	private static final String TEST_SPLIT_TABLE = "pxf_accumulo_ext_testsplittable";
	private static boolean enableTests = true;

	@BeforeClass
	public static void setupClass() throws Exception {

		Configuration conf = new Configuration(false);
		conf.addResource(new FileInputStream("src/test/resources/accumulo.conf"));

		enableTests = conf.getBoolean("enable.tests", true);

		if (!enableTests) {
			System.out.println("Tests are disabled");
			return;
		}

		String instanceName = conf.get("accumulo.instance");
		String quorum = conf.get("accumulo.quorum");
		String user = conf.get("accumulo.user");
		String password = conf.get("accumulo.password");

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("recordkey", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("cf:a", DataType.INTEGER));
		columnDefs.add(new Pair<String, DataType>("cf:b", DataType.INTEGER));
		columnDefs.add(new Pair<String, DataType>("cf:c", DataType.INTEGER));

		extraParams.add(new Pair<String, String>("INSTANCE", instanceName));
		extraParams.add(new Pair<String, String>("QUORUM", quorum));

		extraParams.add(new Pair<String, String>("USER", user));
		extraParams.add(new Pair<String, String>("PASSWORD", password));
		extraParams.add(new Pair<String, String>("AUTHS", "public"));

		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, quorum);
		conn = instance.getConnector(user, new PasswordToken(password));

		try {
			conn.tableOperations().delete(DNE_TABLE);
		} catch (Exception e) {
		}
		try {
			conn.tableOperations().delete(EMPTY_TABLE);
		} catch (Exception e) {
		}
		try {
			conn.tableOperations().delete(TEST_TABLE);
		} catch (Exception e) {
		}
		try {
			conn.tableOperations().delete(TEST_SPLIT_TABLE);
		} catch (Exception e) {
		}

		conn.tableOperations().create(EMPTY_TABLE);
		conn.tableOperations().create(TEST_TABLE);

		BatchWriter wrtr = conn.createBatchWriter(TEST_TABLE,
				new BatchWriterConfig());

		Mutation m = new Mutation("row1");
		m.put("cf", "a", System.currentTimeMillis(), Integer.toString(1));
		m.put("cf", "b", System.currentTimeMillis(), Integer.toString(2));
		wrtr.addMutation(m);

		m = new Mutation("row2");
		m.put("cf", "b", System.currentTimeMillis(), Integer.toString(3));
		m.put("cf", "c", System.currentTimeMillis(), Integer.toString(4));
		wrtr.addMutation(m);

		m = new Mutation("row3");
		m.put("cf", "a", System.currentTimeMillis(), Integer.toString(5));
		m.put("cf", "c", System.currentTimeMillis(), Integer.toString(6));
		wrtr.addMutation(m);

		wrtr.flush();
		wrtr.close();

		SortedSet<Text> partitionKeys = new TreeSet<Text>();
		partitionKeys.add(new Text("f"));
		conn.tableOperations().create(TEST_SPLIT_TABLE);
		conn.tableOperations().addSplits(TEST_SPLIT_TABLE, partitionKeys);

		wrtr = conn
				.createBatchWriter(TEST_SPLIT_TABLE, new BatchWriterConfig());

		m = new Mutation("a");
		m.put("cf", "a", System.currentTimeMillis(), Integer.toString(1));
		m.put("cf", "b", System.currentTimeMillis(), Integer.toString(2));
		wrtr.addMutation(m);

		m = new Mutation("f");
		m.put("cf", "b", System.currentTimeMillis(), Integer.toString(3));
		m.put("cf", "c", System.currentTimeMillis(), Integer.toString(4));
		wrtr.addMutation(m);

		m = new Mutation("z");
		m.put("cf", "a", System.currentTimeMillis(), Integer.toString(5));
		m.put("cf", "c", System.currentTimeMillis(), Integer.toString(6));
		wrtr.addMutation(m);

		wrtr.flush();

		wrtr.close();
	}

	@AfterClass
	public static void cleanupClass() throws Exception {
		if (conn != null) {
			conn.tableOperations().delete(EMPTY_TABLE);
			conn.tableOperations().delete(TEST_TABLE);
			conn.tableOperations().delete(TEST_SPLIT_TABLE);
		}
	}

	@Test(expected = IOException.class)
	public void testNonexistentTable() throws Exception {

		if (!enableTests) {
			throw new IOException("Tests are disabled");
		}

		super.assertOutput(new Path(DNE_TABLE), new ArrayList<String>());
	}

	@Test
	public void testEmptyTable() throws Exception {
		if (!enableTests) {
			return;
		}
		super.assertOutput(new Path(EMPTY_TABLE), new ArrayList<String>());
	}

	@Test
	public void testSimpleTable() throws Exception {
		if (!enableTests) {
			return;
		}

		List<String> output = new ArrayList<String>();

		output.add("row1,1,2,");
		output.add("row2,,3,4");
		output.add("row3,5,,6");

		super.assertOutput(new Path(TEST_TABLE), output);
	}

	@Test
	public void testSplitTable() throws Exception {
		if (!enableTests) {
			return;
		}
		List<String> output = new ArrayList<String>();

		output.add("a,1,2,");
		output.add("f,,3,4");
		output.add("z,5,,6");

		super.assertUnorderedOutput(new Path(TEST_SPLIT_TABLE), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return AccumuloFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return AccumuloAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return AccumuloResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
