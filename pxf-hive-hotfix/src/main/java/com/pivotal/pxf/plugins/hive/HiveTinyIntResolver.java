package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.BadRecordException;
import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.UnsupportedTypeException;
import com.pivotal.pxf.api.UserDataException;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class HiveTinyIntResolver extends Plugin implements ReadResolver {
	private SerDe deserializer;
	private List<OneField> partitionFields;
	private String serdeName;
	private String propsString;
	private String partitionKeys;
	private static long timeToConvert = 0;

	public HiveTinyIntResolver(InputData input) throws Exception {
		super(input);

		ParseUserData(input);
		InitSerde();
		InitPartitionFields();
		timeToConvert = 0;
	}

	public static long getTimeToConvert() {
		return timeToConvert;
	}

	private void ParseUserData(InputData input) throws Exception {
		int EXPECTED_NUM_OF_TOKS = 4;

		String userData = new String(input.getFragmentUserData());
		String[] toks = userData.split("!HUDD!");
		if (toks.length != EXPECTED_NUM_OF_TOKS) {
			throw new UserDataException(
					"HiveResolver expected 4 tokens, but got " + toks.length);
		}
		this.serdeName = toks[1];
		this.propsString = toks[2];
		this.partitionKeys = toks[3];
	}

	public List<OneField> getFields(OneRow onerow) throws Exception {
		long start = System.currentTimeMillis();
		List<OneField> record = new LinkedList<OneField>();

		Object tuple = this.deserializer.deserialize((Writable) onerow
				.getData());
		ObjectInspector oi = this.deserializer.getObjectInspector();

		traverseTuple(tuple, oi, record);

		addPartitionKeyValues(record);

		long end = System.currentTimeMillis();

		timeToConvert += (end - start);
		return record;
	}

	private void InitSerde() throws Exception {
		Class<?> c = Class.forName(this.serdeName, true,
				JavaUtils.getClassLoader());
		this.deserializer = ((SerDe) c.newInstance());
		Properties serdeProperties = new Properties();
		ByteArrayInputStream inStream = new ByteArrayInputStream(
				this.propsString.getBytes());
		serdeProperties.load(inStream);
		this.deserializer.initialize(new JobConf(new Configuration(),
				HiveResolver.class), serdeProperties);
	}

	private void InitPartitionFields() {
		this.partitionFields = new LinkedList<OneField>();
		if (this.partitionKeys.compareTo("!HNPT!") == 0) {
			return;
		}
		String[] partitionLevels = this.partitionKeys.split("!HPAD!");
		for (String partLevel : partitionLevels) {
			String[] levelKey = partLevel.split("!H1PD!");
			String type = levelKey[1];
			String val = levelKey[2];
			switch (type) {
			case "string":
				addOneFieldToRecord(this.partitionFields, DataType.TEXT, val);
				break;
			case "tinyint":
				addOneFieldToRecord(this.partitionFields, DataType.SMALLINT,
						Short.valueOf(Short.parseShort(val)));
				break;
			case "smallint":
				addOneFieldToRecord(this.partitionFields, DataType.SMALLINT,
						Short.valueOf(Short.parseShort(val)));
				break;
			case "int":
				addOneFieldToRecord(this.partitionFields, DataType.INTEGER,
						Integer.valueOf(Integer.parseInt(val)));
				break;
			case "bigint":
				addOneFieldToRecord(this.partitionFields, DataType.BIGINT,
						Long.valueOf(Long.parseLong(val)));
				break;
			case "float":
				addOneFieldToRecord(this.partitionFields, DataType.REAL,
						Float.valueOf(Float.parseFloat(val)));
				break;
			case "double":
				addOneFieldToRecord(this.partitionFields, DataType.FLOAT8,
						Double.valueOf(Double.parseDouble(val)));
				break;
			case "timestamp":
				addOneFieldToRecord(this.partitionFields, DataType.TIMESTAMP,
						Timestamp.valueOf(val));
				break;
			case "decimal":
				addOneFieldToRecord(this.partitionFields, DataType.NUMERIC,
						new HiveDecimal(val).bigDecimalValue().toString());
				break;
			default:
				throw new UnsupportedTypeException("Unknown type: " + type);
			}
		}
	}

	private void addPartitionKeyValues(List<OneField> record) {
		for (OneField field : this.partitionFields) {
			record.add(field);
		}
	}

	public void traverseTuple(Object obj, ObjectInspector objInspector,
			List<OneField> record) throws IOException, BadRecordException {
		ObjectInspector.Category category = objInspector.getCategory();
		if ((obj == null) && (category != ObjectInspector.Category.PRIMITIVE)) {
			throw new BadRecordException("NULL Hive composite object");
		}
		List<?> list;
		ObjectInspector eoi;
		ObjectInspector koi;
		ObjectInspector voi;
		switch (category) {
		case PRIMITIVE:
			resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector,
					record);
			break;
		case LIST:
			ListObjectInspector loi = (ListObjectInspector) objInspector;
			list = loi.getList(obj);
			eoi = loi.getListElementObjectInspector();
			if (list == null) {
				throw new BadRecordException(
						"Illegal value NULL for Hive data type List");
			}
			for (Object object : list) {
				traverseTuple(object, eoi, record);
			}
			break;
		case MAP:
			MapObjectInspector moi = (MapObjectInspector) objInspector;
			koi = moi.getMapKeyObjectInspector();
			voi = moi.getMapValueObjectInspector();
			Map<?, ?> map = moi.getMap(obj);
			if (map == null) {
				throw new BadRecordException(
						"Illegal value NULL for Hive data type Map");
			}
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				traverseTuple(entry.getKey(), koi, record);
				traverseTuple(entry.getValue(), voi, record);
			}
			break;
		case STRUCT:
			StructObjectInspector soi = (StructObjectInspector) objInspector;
			List<? extends StructField> fields = soi.getAllStructFieldRefs();
			list = soi.getStructFieldsDataAsList(obj);
			if (list == null) {
				throw new BadRecordException(
						"Illegal value NULL for Hive data type Struct");
			}
			for (int i = 0; i < list.size(); i++) {
				traverseTuple(
						list.get(i),
						((StructField) fields.get(i)).getFieldObjectInspector(),
						record);
			}
			break;
		case UNION:
			UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
			List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
			if (ois == null) {
				throw new BadRecordException(
						"Illegal value NULL for Hive data type Union");
			}
			traverseTuple(uoi.getField(obj),
					(ObjectInspector) ois.get(uoi.getTag(obj)), record);
			break;
		default:
			throw new UnsupportedTypeException("Unknown category type: "
					+ objInspector.getCategory());
		}
	}

	public void resolvePrimitive(Object o, PrimitiveObjectInspector oi,
			List<OneField> record) throws IOException {
		Object val;
		switch (oi.getPrimitiveCategory()) {
		case BYTE:
			val = null;

			val = o != null ? Short.valueOf(((ByteObjectInspector) oi).get(o))
					: null;
			addOneFieldToRecord(record, DataType.SMALLINT, val);
			break;
		case BOOLEAN:
			val = o != null ? Boolean.valueOf(((BooleanObjectInspector) oi)
					.get(o)) : null;
			addOneFieldToRecord(record, DataType.BOOLEAN, val);
			break;
		case SHORT:
			val = o != null ? Short.valueOf(((ShortObjectInspector) oi).get(o))
					: null;
			addOneFieldToRecord(record, DataType.SMALLINT, val);
			break;
		case INT:
			val = o != null ? Integer.valueOf(((IntObjectInspector) oi).get(o))
					: null;
			addOneFieldToRecord(record, DataType.INTEGER, val);
			break;
		case LONG:
			val = o != null ? Long.valueOf(((LongObjectInspector) oi).get(o))
					: null;
			addOneFieldToRecord(record, DataType.BIGINT, val);
			break;
		case FLOAT:
			val = o != null ? Float.valueOf(((FloatObjectInspector) oi).get(o))
					: null;
			addOneFieldToRecord(record, DataType.REAL, val);
			break;
		case DOUBLE:
			val = o != null ? Double.valueOf(((DoubleObjectInspector) oi)
					.get(o)) : null;
			addOneFieldToRecord(record, DataType.FLOAT8, val);
			break;
		case DECIMAL:
			String sVal = null;
			if (o != null) {
				BigDecimal bd = ((HiveDecimalObjectInspector) oi)
						.getPrimitiveJavaObject(o).bigDecimalValue();
				sVal = bd.toString();
			}
			addOneFieldToRecord(record, DataType.NUMERIC, sVal);
			break;
		case STRING:
			val = o != null ? ((StringObjectInspector) oi)
					.getPrimitiveJavaObject(o) : null;
			addOneFieldToRecord(record, DataType.TEXT, val);
			break;
		case BINARY:
			byte[] toEncode = null;
			if (o != null) {
				BytesWritable bw = ((BinaryObjectInspector) oi)
						.getPrimitiveWritableObject(o);
				toEncode = new byte[bw.getLength()];
				System.arraycopy(bw.getBytes(), 0, toEncode, 0, bw.getLength());
			}
			addOneFieldToRecord(record, DataType.BYTEA, toEncode);
			break;
		case TIMESTAMP:
			val = o != null ? ((TimestampObjectInspector) oi)
					.getPrimitiveJavaObject(o) : null;
			addOneFieldToRecord(record, DataType.TIMESTAMP, val);
			break;
		default:
			throw new UnsupportedTypeException(oi.getTypeName()
					+ " conversion is not supported by "
					+ getClass().getSimpleName());
		}
	}

	void addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType,
			Object val) {
		OneField oneField = new OneField();
		oneField.type = gpdbWritableType.getOID();
		oneField.val = val;
		record.add(oneField);
	}
}
