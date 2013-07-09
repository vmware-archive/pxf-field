package com.emc.greenplum.gpdb.hdfsconnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccumuloResolver implements IFieldsResolver {

	private HDFSMetaData conf;

	public AccumuloResolver(HDFSMetaData meta) {
		conf = meta;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<OneField> GetFields(OneRow onerow) throws Exception {
		System.out.println("GetFields");

		List<OneField> fields = new ArrayList<OneField>();

		Map<String, byte[]> keyValues = (Map<String, byte[]>) onerow.getData();

		for (int i = 0; i < this.conf.columns(); ++i) {
			ColumnDescriptor column = (ColumnDescriptor) this.conf.getColumn(i);

			byte[] value = keyValues.get(column.columnName());
			if (value != null) {

				OneField oneField = new OneField();
				oneField.type = column.gpdbColumnType;
				oneField.val = convertToJavaObject(oneField.type, value);

				fields.add(oneField);
			} else {
				OneField oneField = new OneField();
				oneField.type = column.gpdbColumnType;
				oneField.val = null;
				fields.add(oneField);
			}
		}

		return fields;
	}

	Object convertToJavaObject(int type, byte[] val) throws Exception {
		if (val == null)
			return null;
		try {
			switch (type) {
			case 25:
			case 1042:
			case 1043:
				return new String(val);

			case 23:
				return Integer.valueOf(Integer.parseInt(new String(val)));

			case 20:
				return Long.valueOf(Long.parseLong(new String(val)));

			case 21:
				return Short.valueOf(Short.parseShort(new String(val)));

			case 700:
				return Float.valueOf(Float.parseFloat(new String(val)));

			case 701:
				return Double.valueOf(Double.parseDouble(new String(val)));

			case 17:
				return val;
			}

			throw new Exception("GPHBase doesn't support data type " + type
					+ " yet");

		} catch (NumberFormatException e) {
			throw new BadRecordException();
		}
	}
}
