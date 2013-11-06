package com.pivotal.pxf.accessors;

import java.nio.ByteBuffer;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.gopivotal.cassandra.ColumnFamilyInputFormat;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

public class CassandraAccessor extends Plugin implements IReadAccessor {

	private Configuration conf = new Configuration();
	private JobConf jobConf = null;

	// Connecting to Cassandra
	private String address, keyspaceName, columnFamily, partitioner;


	private ByteBuffer key;
	private SortedMap<ByteBuffer, IColumn> value;

	private ColumnFamilyInputFormat format = null;
	private RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> reader = null;

	public CassandraAccessor(InputData inputData) throws Exception {
		super(inputData);

		keyspaceName = inputData.getProperty("X-GP-DATA-DIR");
		address = inputData.getProperty("X-GP-ADDRESS");
		columnFamily = inputData.getProperty("X-GP-COLUMN-FAMILY");
		partitioner = inputData.getProperty("X-GP-PARTITIONER");
		jobConf = new JobConf(conf, CassandraAccessor.class);

		ConfigHelper.setInputColumnFamily(jobConf, keyspaceName, columnFamily);
		ConfigHelper.setInputInitialAddress(jobConf, address);
		ConfigHelper.setInputPartitioner(jobConf, partitioner);

		// full slice predicates
		SlicePredicate p = new SlicePredicate();
		SliceRange r = new SliceRange(ByteBuffer.wrap(new byte[0]),
				ByteBuffer.wrap(new byte[0]), false, Integer.MAX_VALUE);
		p.setSlice_range(r);

		ConfigHelper.setInputSlicePredicate(jobConf, p);

		format = new ColumnFamilyInputFormat();
	}

	@Override
	public boolean openForRead() throws Exception {

		InputSplit[] splits = format.getSplits(this.jobConf, 1);
		int actual_num_of_splits = splits.length;

		int needed_split_idx = this.inputData.getDataFragment();

		if ((needed_split_idx != -1)
				&& (needed_split_idx < actual_num_of_splits)) {

			reader = format.getRecordReader(splits[needed_split_idx], jobConf, null);
			key = this.reader.createKey();
			value = this.reader.createValue();

			return true;
		} else {
			return false;
		}
	}

	@Override
	public OneRow readNextObject() throws Exception {
		OneRow retval = null;
		
		if ((this.reader.next(this.key, this.value))) {
			retval = new OneRow(this.key, this.value);
		}

		return retval;
	}

	@Override
	public void closeForRead() throws Exception {
		if (this.reader != null) {
			this.reader.close();
		}
	}
}