package com.pivotal.pxf.plugins.pipes;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gopivotal.mapred.input.WholeFileInputFormat;
import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;

/**
 * A WholeFileFragmenter, which creates a single Fragment for every input file
 */
public class WholeFileFragmenter extends Fragmenter {

	private Path inputPath = null;
	private JobConf conf = new JobConf();
	private FileInputFormat<?, ?> format = null;

	public WholeFileFragmenter(InputData data) throws InstantiationException,
			IllegalAccessException {
		super(data);

		inputPath = new Path(data.path());

		format = new WholeFileInputFormat();
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

				filepath = filepath.substring(1);
				Fragment fragment = new Fragment(filepath, hosts,
						fragmentMetadata);
				super.fragments.add(fragment);
			}
		}
		return super.fragments;
	}

	private InputSplit[] getSplits(Path path) throws IOException {
		FileInputFormat.addInputPath(this.conf, path);
		return format.getSplits(this.conf, 1);
	}
}
