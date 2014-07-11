package com.pivotal.pxf.plugins.redis;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;

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
	public List<Fragment> getFragments() throws Exception {

		List<Fragment> fragments = new ArrayList<Fragment>();
		for (String host : params.getHosts()) {
			fragments.add(new Fragment(host,
					new String[] { host.contains(":") ? host.substring(0,
							host.indexOf(":")) : host }, new byte[0]));
		}

		return fragments;
	}
}
