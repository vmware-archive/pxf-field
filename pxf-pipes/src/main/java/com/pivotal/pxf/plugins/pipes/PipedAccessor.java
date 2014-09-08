package com.pivotal.pxf.plugins.pipes;

import org.apache.log4j.Logger;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class PipedAccessor extends Plugin implements ReadAccessor {

	private ReadAccessor impl = null;
	private static final Logger LOG = Logger.getLogger(PipedAccessor.class);

	public PipedAccessor(InputData input) throws Exception {
		super(input);

		if (PxfPipesUtil.isLineByLine(input)) {
			impl = new PipedLineAccessor(input);
		} else {
			impl = new PipedBlobAccessor(input);
		}
	}

	@Override
	public OneRow readNextObject() throws Exception {
		return impl.readNextObject();
	}

	@Override
	public boolean openForRead() throws Exception {
		return impl.openForRead();
	}

	@Override
	public void closeForRead() throws Exception {
		impl.closeForRead();
	}
}
