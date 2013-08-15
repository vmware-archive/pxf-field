package com.pivotal.pxf.accessors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.ListIterator;
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

public class CassandraAccessor extends Accessor {

	private Configuration conf = new Configuration();
	private JobConf jobConf = null;

	// Connecting to Cassandra
	private String address, keyspaceName, columnFamily, partitioner;

	private InputSplit currSplit = null;
	private LinkedList<InputSplit> segSplits = null;
	private ListIterator<InputSplit> iter = null;

	private ByteBuffer key;
	private SortedMap<ByteBuffer, IColumn> value;

	private ColumnFamilyInputFormat format = null;
	private RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> reader = null;
	private InputData metaData = null;

	public CassandraAccessor(InputData meta) throws Exception {
		super(meta);

		this.metaData = meta;

		keyspaceName = meta.getProperty("X-GP-DATA-DIR");
		address = meta.getProperty("X-GP-ADDRESS");
		columnFamily = meta.getProperty("X-GP-COLUMN-FAMILY");
		partitioner = meta.getProperty("X-GP-PARTITIONER");
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

	public boolean Open() throws Exception {

		InputSplit[] splits = format.getSplits(this.jobConf, 0);

		int actual_splits_size = splits.length;
		int allocated_splits_size = this.metaData.dataFragmentsSize();

		this.segSplits = new LinkedList<InputSplit>();
		for (int i = 0; i < allocated_splits_size; ++i) {
			int alloc_split_idx = this.metaData.getDataFragment(i);

			if (alloc_split_idx < actual_splits_size) {
				this.segSplits.add(splits[alloc_split_idx]);
			}
		}

		this.iter = this.segSplits.listIterator(0);

		return getNextSplit();
	}

	public OneRow LoadNextObject() throws IOException {

		OneRow retval = null;
		do {
			if ((this.reader.next(this.key, this.value))) {
				retval = new OneRow(this.key, this.value);
				break;
			} else if (getNextSplit()) {
				continue;
			} else {
				break;
			}

		} while (true);

		return retval;
	}

	private boolean getNextSplit() throws IOException {

		if (!(this.iter.hasNext())) {
			return false;
		}

		currSplit = ((InputSplit) this.iter.next());
		reader = format.getRecordReader(this.currSplit, jobConf, null);
		key = this.reader.createKey();
		value = this.reader.createValue();

		return true;
	}

	public void Close() throws Exception {
		if (this.reader != null) {
			this.reader.close();
		}
	}
}