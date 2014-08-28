package com.pivotal.pxf.plugins.pipes;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class PipedResolver extends Plugin implements ReadResolver {

	private ReadResolver impl = null;

	ExecutorService service = Executors.newFixedThreadPool(2);

	public PipedResolver(InputData data) throws Exception {
		super(data);

		try {
			// if this is a mapper class, then make it a mapper resolver
			PxfPipesUtil.getMapperClass(data);
			impl = new PipedMapperResolver(data);

		} catch (ClassNotFoundException e) {
			// assume it is a command
			impl = new PipedCommandResolver(data);
		}
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		return impl.getFields(row);
	}
}
