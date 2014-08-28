package com.pivotal.pxf.plugins.pipes;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

public class WholeFileAccessor extends HdfsSplittableDataAccessor {

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
}
