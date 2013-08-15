package com.gopivotal.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.mapreduce.template.BaseAnalyticDriver;

public class CassandraMRClient extends BaseAnalyticDriver {

	public static char CF = 'c';
	public static char INITIAL_ADDRESS = 'a';

	@Override
	protected void preJobRun(Job job, CommandLine line) throws Exception {

		ConfigHelper.setInputColumnFamily(job.getConfiguration(),
				line.getOptionValue(BaseAnalyticDriver.INPUT),
				line.getOptionValue(CF));
		ConfigHelper.setInputInitialAddress(job.getConfiguration(),
				line.getOptionValue(INITIAL_ADDRESS));
		ConfigHelper.setInputPartitioner(job.getConfiguration(),
				"org.apache.cassandra.dht.Murmur3Partitioner");
		SlicePredicate p = new SlicePredicate();
		SliceRange r = new SliceRange(ByteBuffer.wrap(new byte[0]),
				ByteBuffer.wrap(new byte[0]), false, Integer.MAX_VALUE);
		p.setSlice_range(r);

		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), p);

		FileSystem.get(job.getConfiguration()).delete(
				new Path(line.getOptionValue(BaseAnalyticDriver.OUTPUT)), true);
	}

	@Override
	protected void postJobRun(Job job) throws Exception {

	}

	@Override
	protected String getJobName() {
		return "Cassandra Test";
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Mapper> getMapperClass() {
		return CassandraMapper.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends InputFormat> getInputFormatClass() {
		return ColumnFamilyInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends WritableComparable> getOutputKeyClass() {
		return Text.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends WritableComparable> getOutputValueClass() {
		return Text.class;
	}

	@SuppressWarnings("static-access")
	@Override
	protected Iterable<Option> getExtraOptions() {

		List<Option> opts = new ArrayList<Option>();

		opts.add(OptionBuilder.isRequired().hasArg()
				.withLongOpt("column-family")
				.withDescription("Cassandra ColumnFamily").create(CF));
		opts.add(OptionBuilder.isRequired().hasArg().withLongOpt("address")
				.withDescription("Cassandra initial address")
				.create(INITIAL_ADDRESS));

		return opts;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(),
				new CassandraMRClient(), args));
	}
}
