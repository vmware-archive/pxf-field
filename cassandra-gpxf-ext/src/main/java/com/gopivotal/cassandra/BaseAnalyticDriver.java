package com.gopivotal.cassandra;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class BaseAnalyticDriver extends Configured implements Tool {

	public static char INPUT = 'i';
	public static char OUTPUT = 'o';
	public static char NUM_REDUCE_TASKS = 'r';
	private CommandLine line = null;

	@Override
	public int run(String[] args) throws Exception {

		line = getCommandLine(args);
		if (line == null) {
			return 1;
		}

		Job job = Job.getInstance(getConf(), getJobName());
		job.setJarByClass(getClass());

		job.setMapperClass(getMapperClass());
		job.setCombinerClass(getCombinerClass());
		job.setCombinerClass(getReducerClass());

		job.setNumReduceTasks(getNumReduceTasks());

		job.setInputFormatClass(getInputFormatClass());
		job.setOutputFormatClass(getOutputFormatClass());

		setInputPaths(job, line);
		setOutputPath(job, line);

		job.setOutputKeyClass(getOutputKeyClass());
		job.setOutputValueClass(getOutputValueClass());

		preJobRun(job, line);

		int retval = job.waitForCompletion(true) ? 0 : 1;

		postJobRun(job);

		return retval;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Mapper> getMapperClass() {
		return Mapper.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getCombinerClass() {
		return Reducer.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getReducerClass() {
		return Reducer.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends InputFormat> getInputFormatClass() {
		return TextInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return TextOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends WritableComparable> getOutputKeyClass() {
		return LongWritable.class;
	}

	protected Class<? extends Writable> getOutputValueClass() {
		return Text.class;
	}

	protected abstract void preJobRun(Job job, CommandLine line)
			throws Exception;

	protected abstract void postJobRun(Job job) throws Exception;

	protected abstract String getJobName();

	@SuppressWarnings("unchecked")
	protected Iterable<Option> getExtraOptions() {
		return Collections.EMPTY_LIST;
	}

	protected int getNumReduceTasks() {
		if (line.hasOption(NUM_REDUCE_TASKS)) {
			return Integer.parseInt(line.getOptionValue(NUM_REDUCE_TASKS));
		} else {
			return Integer.parseInt(getConf().get("mapred.reduce.tasks"));
		}
	}

	private void setInputPaths(Job job, CommandLine line) throws IOException {
		String[] paths = line.getOptionValue(INPUT).split(",");

		for (String path : paths) {
			FileInputFormat.addInputPath(job, new Path(path));
		}
	}

	private void setOutputPath(Job job, CommandLine line) throws IOException {
		FileOutputFormat.setOutputPath(job,
				new Path(line.getOptionValue(OUTPUT)));
	}

	@SuppressWarnings("static-access")
	private CommandLine getCommandLine(String[] args) throws ParseException {
		Options opts = new Options();

		opts.addOption(OptionBuilder.isRequired().hasArg().withLongOpt("input")
				.withDescription("CSV list of paths, wildcard optional")
				.create(INPUT));
		opts.addOption(OptionBuilder.isRequired().hasArg()
				.withLongOpt("output")
				.withDescription("Output directory (must not exist")
				.create(OUTPUT));

		opts.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Print this help message").create('h'));

		for (Option opt : getExtraOptions()) {
			opts.addOption(opt);
		}

		CommandLineParser parser = new BasicParser();

		try {
			CommandLine retval = parser.parse(opts, args);
			if (retval.hasOption('h')) {
				printHelp(opts);
				System.exit(0);
			}

			return retval;

		} catch (ParseException e) {
			System.err.println(e.getMessage());
			printHelp(opts);
			return null;
		}
	}

	private void printHelp(Options opts) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("hadoop jar <jarfile>", opts);
	}
}
