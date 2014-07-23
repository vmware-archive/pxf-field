package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.FilterParser;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class HiveAccessor extends HdfsSplittableDataAccessor {

	private Log Log;
	private List<Partition> partitions;
	private Log LOG = LogFactory.getLog(HiveAccessor.class);
	private long start, end;
	private String path = null;
	private String[] hosts = null;
	private long startBytes, endBytes, accessorTime = 0;

	public HiveAccessor(InputData input) throws Exception {
		super(input, null);
		this.fformat = createInputFormat(input);
		this.Log = LogFactory.getLog(HiveAccessor.class);

		accessorTime = 0;
	}

	public OneRow readNextObject() throws IOException {
		long startRead = System.currentTimeMillis();
		OneRow retval = super.readNextObject();
		long endRead = System.currentTimeMillis();

		accessorTime += (endRead - startRead);

		return retval;
	}

	public boolean openForRead() throws Exception {
		start = System.currentTimeMillis();
		boolean test = isOurDataInsideFilteredPartition();

		if (test) {
			LinkedList<InputSplit> segSplits = new LinkedList<InputSplit>();

			FileSplit fileSplit = HdfsUtilities
					.parseFragmentMetadata(this.inputData);
			segSplits.add(fileSplit);

			this.iter = segSplits.listIterator(0);

			path = fileSplit.getPath().toString();
			hosts = fileSplit.getLocations();

			startBytes = fileSplit.getStart();
			endBytes = startBytes + fileSplit.getLength();

			test = getNextSplit();

			return test;
		} else {
			return false;
		}
	}

	protected Object getReader(JobConf jobConf, InputSplit split)
			throws IOException {
		return this.fformat.getRecordReader(split, jobConf, Reporter.NULL);
	}

	public void closeForRead() throws Exception {
		super.closeForRead();
		end = System.currentTimeMillis();

		long resolverTime = HiveTinyIntResolver.getTimeToConvert();

		LOG.info("STATS2 " + path + " " + (end - start) + " " + startBytes
				+ " " + endBytes + " " + (endBytes - startBytes) + " "
				+ StringUtils.join(hosts, ",") + " " + accessorTime + " "
				+ resolverTime);
	}

	private FileInputFormat<?, ?> createInputFormat(InputData input)
			throws Exception {
		String userData = new String(input.getFragmentUserData());
		String[] toks = userData.split("!HUDD!");
		initPartitionFields(toks[3]);

		return HiveDataFragmenter.makeInputFormat(toks[0], this.jobConf);
	}

	private void initPartitionFields(String partitionKeys) {
		this.partitions = new LinkedList<Partition>();
		if (partitionKeys.compareTo("!HNPT!") == 0) {
			return;
		}

		String[] partitionLevels = partitionKeys.split("!HPAD!");
		for (String partLevel : partitionLevels) {
			String[] levelKey = partLevel.split("!H1PD!");
			String name = levelKey[0];
			String type = levelKey[1];
			String val = levelKey[2];
			this.partitions.add(new Partition(name, type, val));
		}
	}

	private boolean isOurDataInsideFilteredPartition() throws Exception {
		if (!this.inputData.hasFilter()) {
			return true;
		}

		String filterStr = this.inputData.filterString();
		HiveFilterBuilder eval = new HiveFilterBuilder(this.inputData);
		Object filter = eval.getFilterObject(filterStr);

		boolean returnData = isFiltered(this.partitions, filter);

		if (this.Log.isDebugEnabled()) {
			this.Log.debug("segmentId: " + this.inputData.segmentId() + " "
					+ this.inputData.path() + "--" + filterStr + "returnData: "
					+ returnData);
			Iterator<?> i$;
			if ((filter instanceof List))
				for (i$ = ((List<?>) filter).iterator(); i$.hasNext();) {
					Object f = i$.next();
					printOneBasicFilter(f);
				}
			else {
				printOneBasicFilter(filter);
			}
		}

		return returnData;
	}

	private boolean isFiltered(List<Partition> partitionFields, Object filter) {
		if ((filter instanceof List)) {
			for (Iterator<?> i$ = ((List<?>) filter).iterator(); i$.hasNext();) {
				Object f = i$.next();
				if (!testOneFilter(partitionFields, f, this.inputData)) {
					return false;
				}
			}
			return true;
		}

		return testOneFilter(partitionFields, filter, this.inputData);
	}

	private boolean testOneFilter(List<Partition> partitionFields,
			Object filter, InputData input) {
		FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter) filter;

		boolean isFilterOperationEqual = bFilter.getOperation() == FilterParser.Operation.HDOP_EQ;
		if (!isFilterOperationEqual) {
			return true;
		}

		int filterColumnIndex = bFilter.getColumn().index();
		String filterValue = bFilter.getConstant().constant().toString();
		ColumnDescriptor filterColumn = input.getColumn(filterColumnIndex);
		String filterColumnName = filterColumn.columnName();

		for (Partition partition : partitionFields) {
			if (filterColumnName.compareTo(partition.name) == 0) {
				if (filterValue.compareTo(partition.val) == 0) {
					return true;
				}
				return false;
			}

		}

		return true;
	}

	private void printOneBasicFilter(Object filter) {
		FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter) filter;
		boolean isOperationEqual = bFilter.getOperation() == FilterParser.Operation.HDOP_EQ;
		int columnIndex = bFilter.getColumn().index();
		String value = bFilter.getConstant().constant().toString();
		this.Log.debug("isOperationEqual:  " + isOperationEqual
				+ " columnIndex:  " + columnIndex + " value:  " + value);
	}

	public class Partition {
		public String name;
		public String type;
		public String val;

		Partition(String inName, String inType, String inVal) {
			this.name = inName;
			this.type = inType;
			this.val = inVal;
		}
	}
}