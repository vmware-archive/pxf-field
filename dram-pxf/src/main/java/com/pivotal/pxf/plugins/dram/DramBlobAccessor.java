package com.pivotal.pxf.plugins.dram;

import java.io.*;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

/**
 * This accessor, wrapped by PipedAccessor passes the entire contents of
 * the file to the external program. The filename is first passed into the
 * external program, followed by a tab, and then the contents of the file.<br>
 * An example of a Python program to extract pixel RGB values from an image:
 *
 * <pre>
 * #!/usr/bin/python
 * import sys
 * import StringIO
 * from PIL import Image
 *
 * s = sys.stdin.read()
 *
 * idx =  s.index('\t')
 * key = s[0:idx].strip()
 * value = s[idx+1:]
 *
 * buff = StringIO.StringIO() 
 * buff.write(value)
 * buff.seek(0)
 *
 * im = Image.open(buff)
 * pixels = im.load()
 *
 * (width, height) = im.size
 * for x in range(0, width):
 * 	for y in range(0, height):
 * 		(r, g, b) = pixels[x, y]
 * 		print "%s|%s|%s|%s|%s|%s|%s" % (key,x,y,r,g,b)
 *
 * sys.exit(0)
 * </pre>
 */
public class DramBlobAccessor extends HdfsSplittableDataAccessor {
	private static final Logger LOG = Logger.getLogger(DramBlobAccessor.class.getName());

	private long serial=0;

	public DramBlobAccessor(InputData input) throws Exception {
		super(input, new ByteArrayFileInputFormat());

	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {
		try {
			LOG.info("creating ByteArrayFileInputFormat.WholeFileRecordReader()");
			return new ByteArrayFileInputFormat.WholeFileRecordReader(split, conf);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public OneRow readNextObject() throws IOException {
		serial++;
		OneRow superRow = super.readNextObject();
		if(superRow==null){
			return null;
		}
		if (serial % 100000==0) {
			LOG.info(key + " " + serial);
		}
		return new OneRow(key, new Pair(serial, ((BytesWritable)superRow.getData()).getBytes()));
	}
}
