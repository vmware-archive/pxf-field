package com.gopivotal.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.*;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one
 * ColumnFamily.
 * 
 * At minimum, you need to set the CF and predicate (description of columns to
 * extract from each row) in your Hadoop job Configuration. The ConfigHelper
 * class is provided to make this simple: ConfigHelper.setInputColumnFamily
 * ConfigHelper.setInputSlicePredicate
 * 
 * You can also configure the number of rows per InputSplit with
 * ConfigHelper.setInputSplitSize This should be
 * "as big as possible, but no bigger." Each InputSplit is read from Cassandra
 * with multiple get_slice_range queries, and the per-call overhead of
 * get_slice_range is high, so larger split sizes are better -- but if it is too
 * large, you will run out of memory.
 * 
 * The default split size is 64k rows.
 */
public class ColumnFamilyInputFormat
		extends
		AbstractColumnFamilyInputFormat<ByteBuffer, SortedMap<ByteBuffer, IColumn>> {

	public RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		return new ColumnFamilyRecordReader();
	}

	@Override
	protected void validateConfiguration(Configuration conf) {
		super.validateConfiguration(conf);

		if (ConfigHelper.getInputSlicePredicate(conf) == null) {
			throw new UnsupportedOperationException(
					"you must set the predicate with setInputSlicePredicate");
		}
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf jobConf,
			final Reporter reporter) throws IOException {

		TaskAttemptContext tac = new TaskAttemptContextImpl(jobConf, new TaskAttemptID());

		ColumnFamilyRecordReader recordReader = new ColumnFamilyRecordReader(
				jobConf.getInt(CASSANDRA_HADOOP_MAX_KEY_SIZE,
						CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT));
		recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split,
				tac);

		return recordReader;
	}
}
