package com.pivotal.pxf.accessors;

import java.util.HashMap;

import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

public class AccumuloAccessor extends Plugin implements IReadAccessor {

	private Configuration conf = new Configuration();
	private JobConf jobConf = null;

	// Connecting to Accumulo
	private String instanceName, zooKeepers, principal, tableName;
	private PasswordToken token = null;
	private Authorizations auths = null;

	private Key key = new Key();
	private Value value = new Value();

	private AccumuloInputFormat format = null;
	private RecordReader<Key, Value> reader = null;
	private Pair<Key, Value> previousResult = null;
	private boolean hasNext = true;

	public AccumuloAccessor(InputData inputData) throws Exception {
		super(inputData);

		tableName = inputData.getProperty("X-GP-DATA-DIR");
		instanceName = inputData.getProperty("X-GP-INSTANCE");
		zooKeepers = inputData.getProperty("X-GP-QUORUM");
		principal = inputData.getProperty("X-GP-USER");
		token = new PasswordToken(inputData.getProperty("X-GP-PASSWORD"));
		jobConf = new JobConf(conf, AccumuloAccessor.class);

		/*
		 * if (meta.getBoolProperty("X-GP-HAS-FILTER")) { String filterString =
		 * meta.getProperty("X-GP-FILTER"); AccumuloFilterEval eval = new
		 * AccumuloFilterEval(meta); List<Range> ranges =
		 * eval.getRanges(filterString); AccumuloInputFormat.setRanges(jobConf,
		 * ranges); }
		 */

		AccumuloInputFormat.setConnectorInfo(jobConf, principal, token);
		AccumuloInputFormat.setScanAuthorizations(jobConf, auths);
		AccumuloInputFormat.setZooKeeperInstance(jobConf, instanceName,
				zooKeepers);
		AccumuloInputFormat.setInputTableName(jobConf, tableName);

		format = new AccumuloInputFormat();
	}

	@Override
	public boolean openForRead() throws Exception {

		InputSplit[] splits = format.getSplits(this.jobConf, 1);
		int actual_num_of_splits = splits.length;

		int needed_split_idx = this.inputData.getDataFragment();

		if ((needed_split_idx != -1)
				&& (needed_split_idx < actual_num_of_splits)) {
			reader = format.getRecordReader(splits[needed_split_idx], jobConf,
					null);
			key = this.reader.createKey();
			value = this.reader.createValue();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public OneRow readNextObject() throws Exception {

		// early out if we don't have a next value
		if (!hasNext) {
			return null;
		}

		HashMap<String, byte[]> keyValuePairs = new HashMap<String, byte[]>();

		// if the previous result is not null, then we should add the key/value
		// pair from the last time we read
		if (previousResult != null) {

			keyValuePairs.put("recordkey", previousResult.getFirst().getRow()
					.toString().getBytes());

			keyValuePairs.put(previousResult.getFirst().getColumnFamily()
					.toString()
					+ ":"
					+ previousResult.getFirst().getColumnQualifierData()
							.toString(), previousResult.getSecond().get());

		}

		boolean ret = false;

		do {
			// advance reader to next key
			if (!(this.reader.next(this.key, this.value))) {
				// no more splits, return the final key
				hasNext = false;
				break;
			} else {

				// create a result out of the read key/value pair
				Pair<Key, Value> result = new Pair<Key, Value>(
						new Key(this.key), new Value(this.value));

				// check if this new result is equal to the last
				if (previousResult == null
						|| previousResult.getFirst().getRow()
								.equals(result.getFirst().getRow())) {

					keyValuePairs.put("recordkey", result.getFirst().getRow()
							.toString().getBytes());

					keyValuePairs.put(result.getFirst().getColumnFamily()
							.toString()
							+ ":"
							+ result.getFirst().getColumnQualifierData()
									.toString(), result.getSecond().get());
				} else {
					// if it is, then it is time to return
					ret = true;
				}

				previousResult = result;
			}

		} while (!ret);

		// make sure we have at least one field
		if (keyValuePairs.size() > 0) {
			return new OneRow(null, keyValuePairs);
		} else {
			return null;
		}
	}

	@Override
	public void closeForRead() throws Exception {
		if (this.reader != null) {
			this.reader.close();
		}
	}
}