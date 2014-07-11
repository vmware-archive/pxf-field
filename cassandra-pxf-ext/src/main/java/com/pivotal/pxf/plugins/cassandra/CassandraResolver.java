package com.pivotal.pxf.plugins.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.pivotal.pxf.api.BadRecordException;
import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class CassandraResolver extends Plugin implements ReadResolver {

	public CassandraResolver(InputData inputData) {
		super(inputData);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<OneField> getFields(OneRow onerow) throws Exception {
		System.out.println("GetFields");

		List<OneField> fields = new ArrayList<OneField>();

		ByteBuffer keyBuffer = (ByteBuffer) onerow.getKey();
		SortedMap<ByteBuffer, IColumn> keyValues = (SortedMap<ByteBuffer, IColumn>) onerow
				.getData();

		for (int i = 0; i < this.inputData.columns(); ++i) {
			ColumnDescriptor column = (ColumnDescriptor) this.inputData
					.getColumn(i);

			if (column.columnName().equals("recordkey")) {
				OneField oneField = new OneField();
				oneField.type = column.columnTypeCode();
				oneField.val = convertToJavaObject(oneField.type, keyBuffer);
				fields.add(oneField);
			} else {
				IColumn value = keyValues.get(ByteBuffer.wrap(column
						.columnName().getBytes()));

				if (value != null) {

					OneField oneField = new OneField();
					oneField.type = column.columnTypeCode();
					oneField.val = convertToJavaObject(oneField.type,
							value.value());
					fields.add(oneField);
				} else {
					OneField oneField = new OneField();
					oneField.type = column.columnTypeCode();
					oneField.val = null;
					fields.add(oneField);
				}
			}
		}

		return fields;
	}

	public Object convertToJavaObject(int type, ByteBuffer val)
			throws Exception {
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
				return Long.valueOf(Long.parseLong(ByteBufferUtil.string(val)));

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
			throw new BadRecordException(e);
		}
	}
}
