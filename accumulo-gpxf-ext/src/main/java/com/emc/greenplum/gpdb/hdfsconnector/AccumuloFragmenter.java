package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class AccumuloFragmenter extends BaseDataFragmenter {
	private Log Log;
	private String principal, instanceName, zooKeepers;
	private PasswordToken token = null;
	private JobConf jobConf = null;
	private Authorizations auths = null;

	public AccumuloFragmenter(BaseMetaData meta) throws Exception {
		super(meta);
		this.Log = LogFactory.getLog(AccumuloFragmenter.class);

		instanceName = meta.getProperty("X-GP-INSTANCE");
		zooKeepers = meta.getProperty("X-GP-QUORUM");
		principal = meta.getProperty("X-GP-USER");
		token = new PasswordToken(meta.getProperty("X-GP-PASSWORD"));
		jobConf = new JobConf();
		auths = new Authorizations(meta.getProperty("X-GP-AUTHS"));

		/*if (meta.getBoolProperty("X-GP-HAS-FILTER")) {
			String filterString = meta.getProperty("X-GP-FILTER");
			AccumuloFilterEval eval = new AccumuloFilterEval(getColumns(meta));
			List<Range> ranges = eval.getRanges(filterString);
			AccumuloInputFormat.setRanges(jobConf, ranges);
		}*/

		AccumuloInputFormat.setConnectorInfo(jobConf, principal, token);
		AccumuloInputFormat.setScanAuthorizations(jobConf, auths);
		AccumuloInputFormat.setZooKeeperInstance(jobConf, instanceName,
				zooKeepers);
	}

	public void GetFragmentInfos(String datapath) throws Exception {

		AccumuloInputFormat.setInputTableName(jobConf, datapath);

		AccumuloInputFormat format = new AccumuloInputFormat();

		InputSplit[] splits = format.getSplits(jobConf, 0);

		for (InputSplit split : splits) {
			RangeInputSplit fsp = (RangeInputSplit) split;

			FragmentInfo fi = new FragmentInfo(datapath, fsp.getLocations());

			this.fragmentInfos.add(fi);
		}

		this.Log.debug(FragmentInfo.listToString(this.fragmentInfos, datapath));
	}

	/*
	private List<ColumnDescriptor> getColumns(BaseMetaData meta) {
		List<ColumnDescriptor> list = new ArrayList<ColumnDescriptor>();
		int columns = meta.getIntProperty("X-GP-ATTRS");
		for (int i = 0; i < columns; ++i) {
			String columnName = meta.getProperty("X-GP-ATTR-NAME" + i);
			int columnType = meta.getIntProperty("X-GP-ATTR-TYPE" + i);
			ColumnDescriptor column = new ColumnDescriptor(columnName,
					columnType, i);
			list.add(column);
		}
		return list;
	}
	*/
}
