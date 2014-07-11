package com.pivotal.pxf.redis;

import org.apache.commons.cli.MissingArgumentException;

import com.pivotal.pxf.utilities.InputData;

public class Parameters {

	public static String HOSTS_PARAM = "X-GP-HOSTS";
	public static String HASHKEY_PARAM = "X-GP-HASHKEY";

	private String[] hosts = null;
	private String hashKey = null, hostString = null;

	public Parameters(InputData inputData) throws MissingArgumentException {

		if (inputData.getParametersMap().containsKey(Parameters.HOSTS_PARAM)) {
			hostString = inputData.getProperty(Parameters.HOSTS_PARAM);
			hosts = hostString.split(",");
		} else {
			throw new MissingArgumentException(Parameters.HOSTS_PARAM
					+ " is not defined.");
		}

		if (inputData.getParametersMap().containsKey(Parameters.HASHKEY_PARAM)) {
			hashKey = inputData.getProperty(Parameters.HASHKEY_PARAM);
		} else {
			throw new MissingArgumentException(Parameters.HASHKEY_PARAM
					+ " is not defined.");
		}
	}

	public String getHostString() {
		return hostString;
	}

	public String[] getHosts() {
		return hosts;
	}

	public String getHashKey() {
		return hashKey;
	}
}
