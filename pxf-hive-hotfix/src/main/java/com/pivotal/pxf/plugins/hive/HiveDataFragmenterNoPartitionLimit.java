package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

public class HiveDataFragmenterNoPartitionLimitDebug extends Fragmenter {
	private JobConf jobConf;
	private HiveMetaStoreClient client;
	private Log LOG = LogFactory
			.getLog(HiveDataFragmenterNoPartitionLimitDebug.class);
	private static final String HIVE_DEFAULT_DBNAME = "default";
	static final String HIVE_UD_DELIM = "!HUDD!";
	static final String HIVE_1_PART_DELIM = "!H1PD!";
	static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
	static final String HIVE_NO_PART_TBL = "!HNPT!";
	private static final int HIVE_MAX_PARTS = -1;

	public HiveDataFragmenterNoPartitionLimitDebug(InputData md) {
		super(md);
		this.jobConf = new JobConf(new Configuration(),
				HiveDataFragmenterNoPartitionLimitDebug.class);
		initHiveClient();
	}

	public List<Fragment> getFragments() throws Exception {
		TblDesc tblDesc = parseTableQualifiedName(this.inputData.tableName());
		if (tblDesc == null) {
			throw new IllegalArgumentException(
					new StringBuilder()
							.append(this.inputData.tableName())
							.append(" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>")
							.toString());
		}

		fetchTableMetaData(tblDesc);

		return this.fragments;
	}

	private void initHiveClient() {
		try {
			this.client = new HiveMetaStoreClient(new HiveConf());
		} catch (MetaException cause) {
			throw new RuntimeException(new StringBuilder()
					.append("Failed connecting to Hive MetaStore service: ")
					.append(cause.getMessage()).toString(), cause);
		}
	}

	private TblDesc parseTableQualifiedName(String qualifiedName) {
		TblDesc tblDesc = new TblDesc();

		String[] toks = qualifiedName.split("[.]");
		if (toks.length == 1) {
			tblDesc.dbName = HIVE_DEFAULT_DBNAME;
			tblDesc.tableName = toks[0];
		} else if (toks.length == 2) {
			tblDesc.dbName = toks[0];
			tblDesc.tableName = toks[1];
		} else {
			tblDesc = null;
		}

		return tblDesc;
	}

	private void fetchTableMetaData(TblDesc tblDesc) throws Exception {
		Table tbl = this.client.getTable(tblDesc.dbName, tblDesc.tableName);

		String tblType = tbl.getTableType();

		if (this.LOG.isDebugEnabled()) {
			this.LOG.debug(new StringBuilder().append("Table: ")
					.append(tblDesc.dbName).append(".")
					.append(tblDesc.tableName).append(", type: ")
					.append(tblType).toString());
		}

		if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW) {
			throw new UnsupportedOperationException(
					"PXF doesn't support HIVE views");
		}

		List<Partition> partitions = this.client.listPartitions(tblDesc.dbName,
				tblDesc.tableName, (short) HIVE_MAX_PARTS);

		StorageDescriptor descTable = tbl.getSd();
		List<FieldSchema> partitionKeys;
		if (partitions.isEmpty()) {
			Properties props = getSchema(tbl);
			fetchMetaDataForSimpleTable(descTable, props);
		} else {
			partitionKeys = tbl.getPartitionKeys();

			for (Partition partition : partitions) {
				StorageDescriptor descPartition = partition.getSd();
				Properties props = MetaStoreUtils.getSchema(descPartition,
						descTable, null, tblDesc.dbName, tblDesc.tableName,
						partitionKeys);

				fetchMetaDataForPartitionedTable(descPartition, props,
						partition, partitionKeys);
			}
		}
	}

	private static Properties getSchema(Table table) {
		return MetaStoreUtils.getSchema(table.getSd(), table.getSd(),
				table.getParameters(), table.getDbName(), table.getTableName(),
				table.getPartitionKeys());
	}

	private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
			Properties props) throws Exception {
		HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props);
		fetchMetaData(tablePartition);
	}

	private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
			Properties props, Partition partition,
			List<FieldSchema> partitionKeys) throws Exception {
		HiveTablePartition tablePartition = new HiveTablePartition(stdsc,
				props, partition, partitionKeys);
		fetchMetaData(tablePartition);
	}

	@SuppressWarnings("rawtypes")
	private void fetchMetaData(HiveTablePartition tablePartition)
			throws Exception {
		FileInputFormat fformat = makeInputFormat(
				tablePartition.storageDesc.getInputFormat(), this.jobConf);
		FileInputFormat.setInputPaths(this.jobConf, new Path[] { new Path(
				tablePartition.storageDesc.getLocation()) });
		InputSplit[] splits = fformat.getSplits(this.jobConf, 0);

		for (InputSplit split : splits) {
			FileSplit fsp = (FileSplit) split;
			String[] hosts = fsp.getLocations();
			String filepath = fsp.getPath().toUri().getPath();
			filepath = filepath.substring(1);

			byte[] locationInfo = HdfsUtilities.prepareFragmentMetadata(fsp);
			Fragment fragment = new Fragment(filepath, hosts, locationInfo,
					makeUserData(tablePartition));
			this.fragments.add(fragment);
		}
	}

	@SuppressWarnings("rawtypes")
	public static FileInputFormat<?, ?> makeInputFormat(String inputFormatName,
			JobConf jobConf) throws Exception {
		Class c = Class.forName(inputFormatName, true,
				JavaUtils.getClassLoader());
		FileInputFormat fformat = (FileInputFormat) c.newInstance();

		if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
			((TextInputFormat) fformat).configure(jobConf);
		}

		return fformat;
	}

	private String serializeProperties(Properties props) throws Exception {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		props.store(outStream, "");
		return outStream.toString();
	}

	private String serializePartitionKeys(HiveTablePartition partData)
			throws Exception {
		if (partData.partition == null) {
			return "!HNPT!";
		}

		StringBuilder partitionKeys = new StringBuilder();
		String prefix = "";
		ListIterator<String> valsIter = partData.partition.getValues()
				.listIterator();
		ListIterator<FieldSchema> keysIter = partData.partitionKeys
				.listIterator();
		while ((valsIter.hasNext()) && (keysIter.hasNext())) {
			FieldSchema key = keysIter.next();
			String name = key.getName();
			String type = key.getType();
			String val = (String) valsIter.next();
			String oneLevel = new StringBuilder().append(prefix).append(name)
					.append("!H1PD!").append(type).append("!H1PD!").append(val)
					.toString();
			partitionKeys.append(oneLevel);
			prefix = "!HPAD!";
		}

		return partitionKeys.toString();
	}

	private byte[] makeUserData(HiveTablePartition partData) throws Exception {
		String inputFormatName = partData.storageDesc.getInputFormat();
		String serdeName = partData.storageDesc.getSerdeInfo()
				.getSerializationLib();
		String propertiesString = serializeProperties(partData.properties);
		String partionKeys = serializePartitionKeys(partData);
		String userData = new StringBuilder().append(inputFormatName)
				.append("!HUDD!").append(serdeName).append("!HUDD!")
				.append(propertiesString).append("!HUDD!").append(partionKeys)
				.toString();

		return userData.getBytes();
	}

	class HiveTablePartition {
		public StorageDescriptor storageDesc;
		public Properties properties;
		public Partition partition;
		public List<FieldSchema> partitionKeys;

		public HiveTablePartition(StorageDescriptor inStorageDesc,
				Properties inProperties) {
			this(inStorageDesc, inProperties, null, null);
		}

		public HiveTablePartition(StorageDescriptor inStorageDesc,
				Properties inProperties, Partition inPartition) {
			this(inStorageDesc, inProperties, inPartition, null);
		}

		public HiveTablePartition(StorageDescriptor inStorageDesc,
				Properties inProperties, Partition inPartition,
				List<FieldSchema> inPartitionKeys) {
			this.storageDesc = inStorageDesc;
			this.properties = inProperties;
			this.partition = inPartition;
			this.partitionKeys = inPartitionKeys;
		}
	}

	class TblDesc {
		public String dbName;
		public String tableName;

		TblDesc() {
		}
	}
}