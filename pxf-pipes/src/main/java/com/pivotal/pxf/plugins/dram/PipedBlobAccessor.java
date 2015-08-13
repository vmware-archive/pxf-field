package com.pivotal.pxf.plugins.dram;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

/**
 * This accessor, wrapped by {@link PipedAccessor} passes the entire contents of
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
public class PipedBlobAccessor extends HdfsSplittableDataAccessor {
	// private static final Logger LOG =
	// Logger.getLogger(PipedBlobAccessor.class);
	private String mapCmd = null;
	private List<String> cmdArgs = null;
	private BlockingQueue<String> rows = null;
	private String currRow = null;
	private PipedCommandThread thread = null;

	public PipedBlobAccessor(InputData input) throws Exception {
		super(input, new WholeFileInputFormat());
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {
		try {
			return new WholeFileInputFormat.WholeFileRecordReader(split, conf);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public OneRow readNextObject() throws IOException {
		if (thread == null) {
			// if thread is null, then this is the first call and we need to
			// start the piped program
			OneRow superRow = super.readNextObject();

			thread = new PipedCommandThread(super.inputData, superRow);
			thread.start();

			// get the container where rows are dumped
			rows = thread.getRows();
		}

		while (true) {
			// poll for a row in the underlying queue
			if ((currRow = rows.poll()) != null) {
				// we have a row, return it
				return new OneRow(null, currRow);
			} else if (!thread.isAlive()) {
				// if the thread is dead, check one last time for race
				// condition
				if ((currRow = rows.poll()) != null) {
					// ha! we have a row!
					return new OneRow(null, currRow);
				} else {
					// jk, return null for done
					return null;
				}
			}
			// else, thread is still alive, so we'll loop back again
		}
	}

	/**
	 * This thread runs the actual piped program in threads, inserting rows of
	 * data into a queue
	 */
	public class PipedCommandThread extends Thread {

		private BlockingQueue<String> rows = null;
		private ProcessBuilder bldr = null;
		private Text key = null;
		private BytesWritable value = null;
		private final byte[] KV_DELIMITER;

		public PipedCommandThread(InputData input, OneRow row) {

			// Initialize the queue to insert rows, based on a configurable
			// maximum
			int queueSize = PxfPipesUtil.getQueueSize(input);

			if (queueSize > 0) {
				rows = new LinkedBlockingQueue<String>(queueSize);
			} else {
				rows = new LinkedBlockingQueue<String>();
			}

			// get custom key/value delimiter, default tab
			KV_DELIMITER = PxfPipesUtil.getKeyValueDelimiter(input);

			// get the underlying mapper command, required -- exception thrown
			// by utility
			mapCmd = PxfPipesUtil.getMapperCommand(input);

			// split up the command
			// TODO support quotes
			cmdArgs = Arrays.asList(mapCmd.split(" "));

			// create the process builder object and get the key/value pair to
			// pass in
			bldr = new ProcessBuilder(cmdArgs);
			key = (Text) row.getKey();
			value = (BytesWritable) row.getData();
		}

		public BlockingQueue<String> getRows() {
			return rows;
		}

		@Override
		public void run() {
			InputStream errStrm = null;
			try {
				// let's start our process and get the error stream
				final Process p = bldr.start();
				errStrm = p.getErrorStream();

				// the writer process writes the key, a tab, and then the value
				Thread writer = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							p.getOutputStream().write(key.getBytes(), 0,
									key.getLength());
							p.getOutputStream().write(KV_DELIMITER, 0,
									KV_DELIMITER.length);
							p.getOutputStream().write(value.getBytes(), 0,
									value.getLength());
							p.getOutputStream().close();

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});

				// our reader tab reads lines of data and adds them to our
				// collection
				Thread reader = new Thread(new Runnable() {
					public void run() {

						try {
							BufferedReader rdr = new BufferedReader(
									new InputStreamReader(p.getInputStream()));
							String line;
							while ((line = rdr.readLine()) != null) {
								try {
									rows.put(line);
								} catch (InterruptedException e) {
									throw new RuntimeException(e);
								}
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				});

				// start our two threads
				writer.start();
				reader.start();

				// wait for the process to complete
				if (p.waitFor() != 0) {
					throw new RuntimeException(
							"Process ended with non-zero exit code: "
									+ p.exitValue());
				}
			} catch (Exception e) {
				// if an exception occurred, log stderr if available
				if (errStrm != null) {
					ByteArrayOutputStream outstrm = new ByteArrayOutputStream();
					try {
						IOUtils.copy(errStrm, outstrm);
						throw new RuntimeException(outstrm.toString(), e);
					} catch (IOException e1) {
						throw new RuntimeException("error in copying stderr", e);
					}
				} else {
					throw new RuntimeException("stderr is null", e);
				}
			}
		}
	}
}
