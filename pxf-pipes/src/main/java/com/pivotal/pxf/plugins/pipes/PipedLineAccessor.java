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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Logger;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

public class PipedLineAccessor extends HdfsSplittableDataAccessor {
	private static final Logger LOG = Logger.getLogger(PipedLineAccessor.class);
	private String mapCmd = null;
	private List<String> cmdArgs = null;
	private BlockingQueue<String> rows = null;
	private String currRow = null;
	private PipedCommandThread thread = null;

	public PipedLineAccessor(InputData input) throws Exception {
		super(input, new TextInputFormat());
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {
		return new LineRecordReader(conf, (FileSplit) split);
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
					// jk, let's get another row and start another thread!

					OneRow superRow = super.readNextObject();

					if (superRow == null) {
						// jk again, return null for odne
						return null;
					}

					thread = new PipedCommandThread(super.inputData, superRow);
					thread.start();

					rows = thread.getRows();
				}
			}
			// else, thread is still alive, so we'll loop back again
		}
	}

	public class PipedCommandThread extends Thread {

		private BlockingQueue<String> rows = new LinkedBlockingQueue<String>();
		private ProcessBuilder bldr = null;
		private LongWritable key = null;
		private Text value = null;
		private final byte[] TAB_BYTES = "\t".getBytes();

		public PipedCommandThread(InputData data, OneRow row) {

			mapCmd = PxfPipesUtil.getMapperCommand(data);

			if (mapCmd == null) {
				throw new InvalidParameterException("Must set MAPPER");
			}
			cmdArgs = Arrays.asList(mapCmd.split(" "));

			bldr = new ProcessBuilder(cmdArgs);
			key = (LongWritable) row.getKey();
			value = (Text) row.getData();
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
							p.getOutputStream().write(
									key.toString().getBytes(), 0,
									key.toString().getBytes().length);
							p.getOutputStream().write(TAB_BYTES, 0,
									TAB_BYTES.length);
							p.getOutputStream().write(
									value.toString().getBytes(), 0,
									value.toString().getBytes().length);
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
