package com.pivotal.pxf.accessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.pivotal.pxf.PxfUnit.Pair;
import com.pivotal.pxf.accessors.Accessor;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;

public class AccumuloAccessor extends Accessor {

	private Configuration conf = new Configuration();
	private JobConf jobConf = null;

	// Connecting to Accumulo
	private String instanceName, zooKeepers, principal, tableName;
	private PasswordToken token = null;
	private Authorizations auths = null;

	private Key key = new Key();
	private Value value = new Value();

	private InputSplit currSplit = null;
	private LinkedList<InputSplit> segSplits = null;
	private ListIterator<InputSplit> iter = null;

	private AccumuloInputFormat format = null;
	private RecordReader<Key, Value> reader = null;
	private InputData metaData = null;
	private Pair<Key, Value> previousResult = null;
	private boolean hasNext = true;

	public AccumuloAccessor(InputData meta) throws Exception {
		super(meta);
		
		this.metaData = meta;

		tableName = meta.getProperty("X-GP-DATA-DIR");
		instanceName = meta.getProperty("X-GP-INSTANCE");
		zooKeepers = meta.getProperty("X-GP-QUORUM");
		principal = meta.getProperty("X-GP-USER");
		token = new PasswordToken(meta.getProperty("X-GP-PASSWORD"));
		jobConf = new JobConf(conf, AccumuloAccessor.class);

		/*if (meta.getBoolProperty("X-GP-HAS-FILTER")) {
			String filterString = meta.getProperty("X-GP-FILTER");
			AccumuloFilterEval eval = new AccumuloFilterEval(meta);
			List<Range> ranges = eval.getRanges(filterString);
			AccumuloInputFormat.setRanges(jobConf, ranges);
		}*/

		AccumuloInputFormat.setConnectorInfo(jobConf, principal, token);
		AccumuloInputFormat.setScanAuthorizations(jobConf, auths);
		AccumuloInputFormat.setZooKeeperInstance(jobConf, instanceName,
				zooKeepers);
		AccumuloInputFormat.setInputTableName(jobConf, tableName);

		format = new AccumuloInputFormat();
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

		// early out if we don't have a next value
		if (!hasNext) {
			return null;
		}

		HashMap<String, byte[]> keyValuePairs = new HashMap<String, byte[]>();

		// if the previous result is not null, then we should add the key/value
		// pair from the last time we read
		if (previousResult != null) {

			keyValuePairs.put("recordkey", previousResult.first.getRow()
					.toString().getBytes());

			keyValuePairs.put(previousResult.first.getColumnFamily().toString()
					+ ":"
					+ previousResult.first.getColumnQualifierData().toString(),
					previousResult.second.get());

		}

		boolean ret = false;

		do {
			// advance reader to next key
			if (!(this.reader.next(this.key, this.value))) {

				// if we exhausted this split, go to the next one and continue
				if (getNextSplit()) {
					continue;
				} else {
					// no more splits, return the final key
					hasNext = false;
					break;
				}
			} else {

				// create a result out of the read key/value pair
				Pair<Key, Value> result = new Pair<Key, Value>(
						new Key(this.key), new Value(this.value));

				// check if this new result is equal to the last
				if (previousResult == null
						|| previousResult.first.getRow().equals(
								result.first.getRow())) {

					keyValuePairs.put("recordkey", result.first.getRow()
							.toString().getBytes());

					keyValuePairs.put(result.first.getColumnFamily().toString()
							+ ":"
							+ result.first.getColumnQualifierData().toString(),
							result.second.get());
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