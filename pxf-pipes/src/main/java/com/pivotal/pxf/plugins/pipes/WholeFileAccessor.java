package com.pivotal.pxf.plugins.pipes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

public class WholeFileAccessor extends HdfsSplittableDataAccessor {

	private static Set<Object> finishedKeys = new HashSet<Object>();

	private Object myKey = null;

	public WholeFileAccessor(InputData input) throws Exception {
		super(input, new WholeFileInputFormat());
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {

		try {
			return new WholeFileInputFormat.WholeFileRecordReader(split, conf);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public OneRow readNextObject() throws IOException {

		OneRow row = super.readNextObject();

		if (row == null) {
			if (isFinished(myKey)) {
				return null;
			} else {
				return new OneRow(myKey, null);
			}
		} else {
			registerKey(row.getKey());
			myKey = row.getKey();
			return row;
		}
	}

	public static synchronized void registerKey(Object key) {
		finishedKeys.add(key);
	}

	public static synchronized boolean isFinished(Object key) {
		return !finishedKeys.contains(key);
	}

	public static synchronized void unregisterKey(Object key) {
		finishedKeys.remove(key);
	}
}
