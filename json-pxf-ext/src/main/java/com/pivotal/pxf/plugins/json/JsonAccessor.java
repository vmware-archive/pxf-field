package com.pivotal.pxf.plugins.json;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.mapred.input.JsonInputFormat;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

/**
 * This JSON accessor for PXF will read JSON data and pass it to a
 * {@link JsonResolver}.
 * 
 * This accessor supports a single JSON record per line, or a more
 * "pretty print" format.
 */
public class JsonAccessor extends HdfsSplittableDataAccessor {

	public static final String IDENTIFIER_PARAM = "X-GP-IDENTIFIER";
	public static final String ONERECORDPERLINE_PARAM = "X-GP-ONERECORDPERLINE";

	private String identifier = "";
	private boolean oneRecordPerLine = true;

	public JsonAccessor(InputData inputData) throws Exception {
		super(inputData, new JsonInputFormat());

		if (inputData.getParametersMap().containsKey(IDENTIFIER_PARAM)) {
			identifier = inputData.getParametersMap().get(IDENTIFIER_PARAM);
		}

		if (inputData.getParametersMap().containsKey(ONERECORDPERLINE_PARAM)) {
			oneRecordPerLine = Boolean.parseBoolean(inputData
					.getParametersMap().get(ONERECORDPERLINE_PARAM));
		}
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {
		conf.set(JsonInputFormat.RECORD_IDENTIFIER, identifier);

		if (oneRecordPerLine) {
			return new JsonInputFormat.SimpleJsonRecordReader(conf,
					(FileSplit) split);
		} else {
			return new JsonInputFormat.JsonRecordReader(conf, (FileSplit) split);
		}
	}
}
