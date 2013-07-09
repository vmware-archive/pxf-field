package com.emc.greenplum.gpdb.hdfsconnector;

import java.nio.ByteBuffer;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.cassandra.ColumnFamilyInputFormat;

public class CassandraFragmenter extends BaseDataFragmenter {

	private Log Log;
	private JobConf jobConf = null;

	// Connecting to Cassandra
	private String address, keyspaceName, columnFamily, partitioner;

	public CassandraFragmenter(BaseMetaData meta) throws Exception {
		super(meta);
		this.Log = LogFactory.getLog(CassandraFragmenter.class);

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

	public void GetFragmentInfos(String datapath) throws Exception {
		
		keyspaceName = datapath;

		ConfigHelper.setInputColumnFamily(jobConf, keyspaceName, columnFamily);

		ColumnFamilyInputFormat format = new ColumnFamilyInputFormat();

		InputSplit[] splits = format.getSplits(jobConf, 0);

		for (InputSplit split : splits) {
			FragmentInfo fi = new FragmentInfo(datapath, split.getLocations());

			this.fragmentInfos.add(fi);
		}

		this.Log.debug(FragmentInfo.listToString(this.fragmentInfos, datapath));
	}
}
