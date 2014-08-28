package com.pivotal.pxf.plugins.pipes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class PipedCommandResolver extends Plugin implements ReadResolver {

	private static Configuration conf = new Configuration();
	private String mapCmd = null;
	private List<String> cmdArgs = null;
	private ProcessBuilder bldr = null;
	private ProcessRunner runner = null;
	private BytesWritable byteObj = null;
	private Text textObj = null;
	private List<OneField> record = new ArrayList<>();

	ExecutorService service = Executors.newFixedThreadPool(2);

	private static final Logger LOG = Logger
			.getLogger(PipedCommandResolver.class);

	public PipedCommandResolver(InputData data) throws Exception {
		super(data);

		mapCmd = PxfPipesUtil.getMapperCommand(data);

		if (mapCmd == null) {
			throw new InvalidParameterException("Must set MAPPER");
		}
		cmdArgs = Arrays.asList(mapCmd.split(" "));

		bldr = new ProcessBuilder(cmdArgs);

		if (data.getProperty("X-GP-FRAGMENTER").equals(
				"com.pivotal.pxf.plugins.pipes.WholeFileFragmenter")) {
			runner = new ProcessRunner() {
				@Override
				public String getOutput(OneRow row) throws Exception {
					byteObj = ((BytesWritable) row.getData());
					return getOutputHelper(byteObj.getBytes(), 0,
							byteObj.getLength());
				}
			};
		} else {
			runner = new ProcessRunner() {
				@Override
				public String getOutput(OneRow row) throws Exception {
					textObj = (Text) row.getData();
					return getOutputHelper(textObj.getBytes(), 0,
							textObj.getLength());
				}
			};
		}
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		record.clear();
		record.add(new OneField(DataType.VARCHAR.getOID(), runner
				.getOutput(row)));
		return record;
	}

	private abstract class ProcessRunner {
		public abstract String getOutput(OneRow row) throws Exception;

		protected String getOutputHelper(final byte[] bytes, final int start,
				final int length) throws Exception {

			final Process p = bldr.start();
			Runnable writer = new Runnable() {
				@Override
				public void run() {
					try {
						LOG.debug("Writing " + length + " bytes");
						p.getOutputStream().write(bytes, start, length);
						p.getOutputStream().close();
						LOG.debug("Closed stream");

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};

			FutureTask<byte[]> reader = new FutureTask<byte[]>(
					new Callable<byte[]>() {
						public byte[] call() throws IOException {
							try {
								ByteArrayOutputStream out = new ByteArrayOutputStream();
								LOG.debug("Reading from stdin");
								IOUtils.copyBytes(p.getInputStream(), out, conf);
								LOG.debug("Done. Read " + out.size());
								return out.toByteArray();
							} catch (IOException e) {
								e.printStackTrace();
								return null;
							}
						}
					});

			service.execute(writer);
			service.execute(reader);

			return new String(reader.get()).trim();
		}
	}
}
