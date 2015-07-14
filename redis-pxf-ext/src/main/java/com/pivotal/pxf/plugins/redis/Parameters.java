package com.pivotal.pxf.plugins.redis;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.lang.StringUtils;

import com.pivotal.pxf.api.utilities.InputData;

public class Parameters {

	public static String HOSTS_PARAM = "HOSTS";
	public static String HASHKEY_PARAM = "HASHKEY";

	private String[] hosts = null;
	private String hashKey = null, hostString = null;

	public Parameters(InputData inputData) throws MissingArgumentException {

		if (!StringUtils.isEmpty(inputData.getUserProperty(HOSTS_PARAM))) {
			hostString = inputData.getUserProperty(HOSTS_PARAM);
			hosts = hostString.split(",");
		} else {
			throw new MissingArgumentException(HOSTS_PARAM + " is not defined.");
		}

		if (!StringUtils.isEmpty(inputData.getUserProperty(HASHKEY_PARAM))) {
			hashKey = inputData.getUserProperty(HASHKEY_PARAM);
		} else {
			throw new MissingArgumentException(HASHKEY_PARAM
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
