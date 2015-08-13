package com.pivotal.pxf.plugins.pipes;

import org.apache.hadoop.mapred.TextInputFormat;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

/**
 * The PipedAccessor class allows the end user to wrap virtually any
 * command-line utility or program with PXF, similar to Hadoop Streaming. This
 * accessor supports line-by-line data via {@link TextInputFormat} and entire
 * files via {@link WholeFileInputFormat}.<br>
 * <br>
 * There are two implementations that are detected at runtime:
 * {@link PipedBlobAccessor} for whole files and {@link PipedLineAccessor} for
 * text files.
 */
public class PipedAccessor extends Plugin implements ReadAccessor {

	private ReadAccessor impl = null;
	//private static final Logger LOG = Logger.getLogger(PipedAccessor.class);

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
