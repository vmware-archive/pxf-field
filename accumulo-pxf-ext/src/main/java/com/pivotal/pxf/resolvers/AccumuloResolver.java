package com.pivotal.pxf.resolvers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.ColumnDescriptor;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

public class AccumuloResolver extends Plugin implements IReadResolver {

	public AccumuloResolver(InputData inputData) {
		super(inputData);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<OneField> getFields(OneRow paramOneRow) throws Exception {

		List<OneField> fields = new ArrayList<OneField>();

		Map<String, byte[]> keyValues = (Map<String, byte[]>) paramOneRow
				.getData();

		for (int i = 0; i < this.inputData.columns(); ++i) {
			ColumnDescriptor column = (ColumnDescriptor) this.inputData
					.getColumn(i);

			byte[] value = keyValues.get(column.columnName());
			if (value != null) {

				OneField oneField = new OneField();
				oneField.type = column.columnTypeCode();
				oneField.val = convertToJavaObject(oneField.type, value);

				fields.add(oneField);
			} else {
				OneField oneField = new OneField();
				oneField.type = column.columnTypeCode();
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
