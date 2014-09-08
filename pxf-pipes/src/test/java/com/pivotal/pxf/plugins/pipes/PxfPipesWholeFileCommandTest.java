package com.pivotal.pxf.plugins.pipes;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.plugins.hdfs.StringPassResolver;

public class PxfPipesWholeFileCommandTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("filename", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("id", DataType.BIGINT));
		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs
				.add(new Pair<String, DataType>("screen_name", DataType.TEXT));
		columnDefs
				.add(new Pair<String, DataType>("hashtags[0]", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("lat", DataType.FLOAT8));
		columnDefs.add(new Pair<String, DataType>("long", DataType.FLOAT8));
	}

	@Before
	public void setup() {
		extraParams.add(new Pair<String, String>("MAPPER", System
				.getProperty("user.dir")
				+ "/src/test/resources/json-wf-mapper.py"));

		extraParams.add(new Pair<String, String>("LINEBYLINE", "FALSE"));
	}

	@After
	public void cleanup() {
		extraParams.clear();
	}

	@Test
	public void testWellFormedJson() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("file:"
				+ System.getProperty("user.dir")
				+ "/src/test/resources/mypptestfile.json|"
				+ "Mon Sep 30 04:04:55 +0000 2013|384529265099689984|I'm craving breadsticks|web|633364307|||");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/mypptestfile.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("file:"
				+ System.getProperty("user.dir")
				+ "/src/test/resources/mypptestfile.json|"
				+ "Mon Sep 30 04:04:55 +0000 2013|384529265099689984|I'm craving breadsticks|web|633364307|||");
		output.add("file:"
				+ System.getProperty("user.dir")
				+ "/src/test/resources/mypptestfile2.json|"
				+ "Mon Sep 30 04:04:55 +0000 2013|384529265099689984|I'm craving breadsticks|web|633364307|||");

		super.assertUnorderedOutput(new Path(System.getProperty("user.dir")
				+ "/" + "src/test/resources/mypptestfile*.json"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return WholeFileFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return PipedAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return StringPassResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
