package com.pivotal.pxf.plugins.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.Configuration;
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

	public static byte[] prepareFragmentMetadata(RangeInputSplit rs)
			throws IOException {
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteArrayStream);
		rs.write(out);

		return byteArrayStream.toByteArray();
	}

	public static RangeInputSplit parseFragmentMetadata(InputData inputData)
			throws IOException {
		byte[] serializedLocation = inputData.getFragmentMetadata();
		if (serializedLocation == null) {
			throw new IllegalArgumentException(
					"Missing fragment location information");
		}

		ByteArrayInputStream bytesStream = new ByteArrayInputStream(
				serializedLocation);
		DataInputStream in = new DataInputStream(bytesStream);

		RangeInputSplit split = new RangeInputSplit();
		split.readFields(in);
		return split;
	}

	public AccumuloFragmenter(InputData meta) throws Exception {
		super(meta);

		instanceName = meta.getProperty("X-GP-INSTANCE");
		zooKeepers = meta.getProperty("X-GP-QUORUM");
		principal = meta.getProperty("X-GP-USER");
		token = new PasswordToken(meta.getProperty("X-GP-PASSWORD"));
		jobConf = new JobConf();
		auths = new Authorizations(meta.getProperty("X-GP-AUTHS"));

		AccumuloInputFormat.setConnectorInfo(jobConf, principal, token);
		AccumuloInputFormat.setScanAuthorizations(jobConf, auths);
		AccumuloInputFormat.setZooKeeperInstance(
				jobConf,
				new ClientConfiguration(new Configuration[0]).withInstance(
						instanceName).withZkHosts(zooKeepers));

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

			output.add(new Fragment(datapath, fsp.getLocations(),
					prepareFragmentMetadata(fsp)));
		}

		return output;
	}
}
