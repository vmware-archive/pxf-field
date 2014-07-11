package com.pivotal.pxf.fragmenters;

import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.FragmentsOutput;
import com.pivotal.pxf.redis.Parameters;
import com.pivotal.pxf.utilities.InputData;

/**
 * This Redis fragmenter for PXF will create one fragment per Redis instance
 */
public class RedisFragmenter extends Fragmenter {

	private Parameters params = null;

	public RedisFragmenter(InputData inputData) throws Exception {
		super(inputData);

		params = new Parameters(inputData);
	}

	@Override
	public FragmentsOutput GetFragments() throws Exception {

		FragmentsOutput fragments = new FragmentsOutput();
		for (String host : params.getHosts()) {
			fragments.addFragment(
					host,
					new String[] { host.contains(":") ? host.substring(0,
							host.indexOf(":")) : host });
		}

		return fragments;
	}
}
