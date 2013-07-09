package com.emc.greenplum.gpdb.hdfsconnector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraResolver implements IFieldsResolver {

	private HDFSMetaData conf;

	public CassandraResolver(HDFSMetaData meta) {
		conf = meta;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<OneField> GetFields(OneRow onerow) throws Exception {
		System.out.println("GetFields");

		List<OneField> fields = new ArrayList<OneField>();

		ByteBuffer keyBuffer = (ByteBuffer) onerow.getKey();
		SortedMap<ByteBuffer, IColumn> keyValues = (SortedMap<ByteBuffer, IColumn>) onerow
				.getData();

		for (int i = 0; i < this.conf.columns(); ++i) {
			ColumnDescriptor column = (ColumnDescriptor) this.conf.getColumn(i);

			if (column.columnName().equals("recordkey")) {
				OneField oneField = new OneField();
				oneField.type = column.gpdbColumnType;
				oneField.val = convertToJavaObject(oneField.type, keyBuffer);
				fields.add(oneField);
			} else {
				IColumn value = keyValues.get(ByteBuffer.wrap(column
						.columnName().getBytes()));

				if (value != null) {

					OneField oneField = new OneField();
					oneField.type = column.gpdbColumnType;
					oneField.val = convertToJavaObject(oneField.type,
							value.value());
					fields.add(oneField);
				} else {
					OneField oneField = new OneField();
					oneField.type = column.gpdbColumnType;
					oneField.val = null;
					fields.add(oneField);
				}
			}
		}

		return fields;
	}

	Object convertToJavaObject(int type, ByteBuffer val) throws Exception {
		if (val == null)
			return null;
		try {
			switch (type) {
			case 25:
			case 1042:
			case 1043:
				return ByteBufferUtil.string(val);

			case 23:
				return Integer.valueOf(Integer.parseInt(ByteBufferUtil
						.string(val)));

			case 20:
				return Long
						.valueOf(Long.parseLong(ByteBufferUtil.string(val)));

			case 21:
				return Short.valueOf(Short.parseShort(ByteBufferUtil
						.string(val)));

			case 700:
				return Float.valueOf(Float.parseFloat(ByteBufferUtil
						.string(val)));

			case 701:
				return Double.valueOf(Double.parseDouble(ByteBufferUtil
						.string(val)));

			case 17:
				return val;
			}

			throw new Exception("GPCassandra doesn't support data type " + type
					+ " yet");

		} catch (NumberFormatException e) {
			throw new BadRecordException();
		}
	}
}
