package com.pivotal.pxf.plugins.pipes;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

public class PipedBlobAccessor extends HdfsSplittableDataAccessor {
	private static final Logger LOG = Logger.getLogger(PipedBlobAccessor.class);
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
			OneRow superRow = super.readNextObject();

			thread = new PipedCommandThread(super.inputData, superRow);
			thread.start();

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

	public class PipedCommandThread extends Thread {

		private BlockingQueue<String> rows = new LinkedBlockingQueue<String>();
		private ProcessBuilder bldr = null;
		private Text key = null;
		private BytesWritable value = null;
		private final byte[] TAB_BYTES = "\t".getBytes();

		public PipedCommandThread(InputData data, OneRow row) {

			mapCmd = PxfPipesUtil.getMapperCommand(data);

			if (mapCmd == null) {
				throw new InvalidParameterException("Must set MAPPER");
			}
			cmdArgs = Arrays.asList(mapCmd.split(" "));

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
				final Process p = bldr.start();
				errStrm = p.getErrorStream();
				Thread writer = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							p.getOutputStream().write(key.getBytes(), 0,
									key.getLength());
							p.getOutputStream().write(TAB_BYTES, 0,
									TAB_BYTES.length);
							p.getOutputStream().write(value.getBytes(), 0,
									value.getLength());
							p.getOutputStream().close();

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});

				Thread reader = new Thread(new Runnable() {
					public void run() {

						try {
							BufferedReader rdr = new BufferedReader(
									new InputStreamReader(p.getInputStream()));
							String line;
							while ((line = rdr.readLine()) != null) {
								rows.add(line);
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				});

				writer.start();
				reader.start();

				if (p.waitFor() != 0) {
					throw new RuntimeException(
							"Process ended with non-zero exit code: "
									+ p.exitValue());
				}
			} catch (Exception e) {
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
