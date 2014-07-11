package com.pivotal.pxf.plugins.accumulo;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;

public class AccumuloFragmenter extends Fragmenter {

	private String principal, instanceName, zooKeepers;
	private PasswordToken token = null;
	private JobConf jobConf = null;
	private Authorizations auths = null;

	public AccumuloFragmenter(InputData meta) throws Exception {
		super(meta);

		instanceName = meta.getProperty("X-GP-INSTANCE");
		zooKeepers = meta.getProperty("X-GP-QUORUM");
		principal = meta.getProperty("X-GP-USER");
		token = new PasswordToken(meta.getProperty("X-GP-PASSWORD"));
		jobConf = new JobConf();
		auths = new Authorizations(meta.getProperty("X-GP-AUTHS"));

		/*
		 * if (meta.getBoolProperty("X-GP-HAS-FILTER")) { String filterString =
		 * meta.getProperty("X-GP-FILTER"); AccumuloFilterEval eval = new
		 * AccumuloFilterEval(getColumns(meta)); List<Range> ranges =
		 * eval.getRanges(filterString); AccumuloInputFormat.setRanges(jobConf,
		 * ranges); }
		 */

		AccumuloInputFormat.setConnectorInfo(jobConf, principal, token);
		AccumuloInputFormat.setScanAuthorizations(jobConf, auths);
		AccumuloInputFormat.setZooKeeperInstance(jobConf, instanceName,
				zooKeepers);
	}

	public void GetFragmentInfos() throws Exception {

	}

	@Override
	public List<Fragment> getFragments() throws Exception {

		String datapath = this.inputData.path();

		datapath = datapath.replaceFirst("^/", "");

		AccumuloInputFormat.setInputTableName(jobConf, datapath);

		AccumuloInputFormat format = new AccumuloInputFormat();

		InputSplit[] splits = format.getSplits(jobConf, 0);

		List<Fragment> output = new ArrayList<Fragment>();

		for (InputSplit split : splits) {
			RangeInputSplit fsp = (RangeInputSplit) split;

			output.add(new Fragment(datapath, fsp.getLocations(), new byte[0]));
		}

		return output;
	}
}
