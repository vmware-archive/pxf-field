package com.pivotal.pxf.plugins.dram;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;

/**
 * A WholeFileFragmenter, which creates a single Fragment for every input file
 */
public class 	WholeFileFragmenter extends Fragmenter {
	private static final Logger LOG = Logger.getLogger(WholeFileFragmenter.class.getName());

	private Path inputPath = null;
	private JobConf conf = new JobConf();
	private FileInputFormat<?, ?> format = null;

	public WholeFileFragmenter(InputData data) throws InstantiationException,
			IllegalAccessException {
		super(data);

		inputPath = new Path(data.getDataSource());
		LOG.info("inputPath:"+inputPath);
		format = new ByteArrayFileInputFormat();
	}

	@Override
	public List<Fragment> getFragments() throws Exception {

		InputSplit[] splits = getSplits(inputPath);
		for (InputSplit split : splits) {
			FileSplit fsp = (FileSplit) split;
			if (fsp.getLength() > 0L) {
				String filepath = fsp.getPath().toUri().getPath();
				String[] hosts = fsp.getLocations();

				byte[] fragmentMetadata = HdfsUtilities
						.prepareFragmentMetadata(fsp);

				//filepath = filepath.substring(1); //TODO import !!!!!!!!!!! remote component error (500) from '192.168.55.202:51200':  type  Exception report   message   File does not exist: /user/pxf/user/pxf/dramdata/sampledata/rawdata.txt.sample.1    description   The server encountered an internal error that prevented it from fulfilling this request.    exception   java.io.IOException: File does not exist: /user/pxf/user/pxf/dramdata/sampledata/rawdata.txt.sample.1 (libchurl.c:852)  (seg0 slice1 phd2.localdomain:40000 pid=85848) (cdbdisp.c:1572)

				LOG.info("filepath:"+filepath);
				Fragment fragment = new Fragment(filepath, hosts,
						fragmentMetadata);
				super.fragments.add(fragment);
			}
		}
		return super.fragments;
	}

	private InputSplit[] getSplits(Path path) throws IOException {
		FileInputFormat.addInputPath(this.conf, path);
		LOG.info("path:" + path);
		return format.getSplits(this.conf, 1);
	}
}
