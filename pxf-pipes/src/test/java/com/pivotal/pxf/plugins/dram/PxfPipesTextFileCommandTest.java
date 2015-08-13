package com.pivotal.pxf.plugins.dram;

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
import com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter;
import com.pivotal.pxf.plugins.hdfs.StringPassResolver;

public class PxfPipesTextFileCommandTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {
		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("id", DataType.BIGINT));
		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs
				.add(new Pair<String, DataType>("screen_name", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("hashtag", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("lat", DataType.FLOAT8));
		columnDefs.add(new Pair<String, DataType>("long", DataType.FLOAT8));
	}

	@Before
	public void setup() {
		extraParams.add(new Pair<String, String>("MAPPER", System
				.getProperty("user.dir")
				+ "/src/test/resources/json-line-mapper.py"));
		extraParams.add(new Pair<String, String>("LINEBYLINE", "TRUE"));
	}

	@After
	public void cleanup() {
		extraParams.clear();
	}

	@Test
	public void testJson() throws Exception {

		List<String> output = new ArrayList<String>();
		output.add("Mon Sep 30 04:04:53 +0000 2013\t384529256681725952\tsigh, who knows.\tweb\t31424214\tCOLUMBUS\t-6.1\t50.103");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260872228864\tI did that 12 years ago..T.T\tweb\t67600981\tKryberWorld\t-8.1\t52.104");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260892786688\tWelp guess I'll have anxiety for another week\tweb\t122795713\tCalifornia\t\t");
		output.add("Mon Sep 30 04:04:55 +0000 2013\t384529265099689984\tI'm craving breadsticks\tweb\t633364307\t\t\t");

		super.assertOutput(new Path(System.getProperty("user.dir")
				+ "/src/test/resources/mytestfile.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Mon Sep 30 04:04:53 +0000 2013\t384529256681725952\tsigh, who knows.\tweb\t31424214\tCOLUMBUS\t-6.1\t50.103");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260872228864\tI did that 12 years ago..T.T\tweb\t67600981\tKryberWorld\t-8.1\t52.104");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260892786688\tWelp guess I'll have anxiety for another week\tweb\t122795713\tCalifornia\t\t");
		output.add("Mon Sep 30 04:04:55 +0000 2013\t384529265099689984\tI'm craving breadsticks\tweb\t633364307\t\t\t");
		output.add("Mon Sep 30 04:04:53 +0000 2013\t384529256681725952\tsigh, who knows.\tweb\t31424214\tCOLUMBUS\t-6.1\t50.103");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260872228864\tI did that 12 years ago..T.T\tweb\t67600981\tKryberWorld\t-8.1\t52.104");
		output.add("Mon Sep 30 04:04:54 +0000 2013\t384529260892786688\tWelp guess I'll have anxiety for another week\tweb\t122795713\tCalifornia\t\t");
		output.add("Mon Sep 30 04:04:55 +0000 2013\t384529265099689984\tI'm craving breadsticks\tweb\t633364307\t\t\t");

		super.assertUnorderedOutput(new Path(System.getProperty("user.dir")
				+ "/" + "src/test/resources/mytestfile*.json"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return HdfsDataFragmenter.class;
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