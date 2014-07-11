package com.pivotal.pxf.plugins.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.cassandra.ColumnFamilyInputFormat;
import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;

public class CassandraFragmenter extends Fragmenter {

	private JobConf jobConf = null;

	// Connecting to Cassandra
	private String address, keyspaceName, columnFamily, partitioner;

	public CassandraFragmenter(InputData meta) throws Exception {
		super(meta);

		address = meta.getProperty("X-GP-ADDRESS");
		columnFamily = meta.getProperty("X-GP-COLUMN-FAMILY");
		partitioner = meta.getProperty("X-GP-PARTITIONER");
		jobConf = new JobConf();

		ConfigHelper.setInputInitialAddress(jobConf, address);
		ConfigHelper.setInputPartitioner(jobConf, partitioner);

		// full slice predicates
		SlicePredicate p = new SlicePredicate();
		SliceRange r = new SliceRange(ByteBuffer.wrap(new byte[0]),
				ByteBuffer.wrap(new byte[0]), false, Integer.MAX_VALUE);
		p.setSlice_range(r);

		ConfigHelper.setInputSlicePredicate(jobConf, p);
	}

	@Override
	public List<Fragment> getFragments() throws Exception {

		keyspaceName = inputData.path();

		ConfigHelper.setInputColumnFamily(jobConf, keyspaceName, columnFamily);

		ColumnFamilyInputFormat format = new ColumnFamilyInputFormat();

		InputSplit[] splits = format.getSplits(jobConf, 0);

		List<Fragment> output = new ArrayList<Fragment>();
		for (InputSplit split : splits) {
			Fragment fragment = new Fragment(keyspaceName,
					split.getLocations(), new byte[0]);
			output.add(fragment);
		}

		return output;
	}
}
