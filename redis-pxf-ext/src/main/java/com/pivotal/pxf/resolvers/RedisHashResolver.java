package com.pivotal.pxf.resolvers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/**
 * This Redis resolver for PXF will decode a given object from the
 * {@link RedisHashAccessor} into a row for HAWQ.
 * 
 * It will also write key value pairs to a specific hash.
 */
public class RedisHashResolver extends Plugin implements IReadResolver {

	private ArrayList<OneField> fields = new ArrayList<OneField>();

	public RedisHashResolver(InputData inputData) throws Exception {
		super(inputData);
	}

	@Override
	public List<OneField> getFields(OneRow paramOneRow) throws Exception {
		fields.clear();

		addFieldFromString(inputData.getColumn(0).columnTypeCode(),
				(String) paramOneRow.getKey());
		addFieldFromString(inputData.getColumn(1).columnTypeCode(),
				(String) paramOneRow.getData());

		return fields;
	}

	private void addFieldFromString(int type, String val) throws IOException {
		OneField oneField = new OneField();
		oneField.type = type;

		if (val == null) {
			oneField.val = null;
		} else {
			switch (type) {
			case GPDBWritable.BIGINT:
				oneField.val = Long.parseLong(val);
				break;
			case GPDBWritable.BOOLEAN:
				oneField.val = Boolean.parseBoolean(val);
				break;
			case GPDBWritable.BPCHAR:
			case GPDBWritable.CHAR:
				oneField.val = val.charAt(0);
				break;
			case GPDBWritable.BYTEA:
				oneField.val = val.getBytes();
				break;
			case GPDBWritable.FLOAT8:
			case GPDBWritable.REAL:
				oneField.val = Double.parseDouble(val);
				break;
			case GPDBWritable.INTEGER:
			case GPDBWritable.SMALLINT:
				oneField.val = Integer.parseInt(val);
				break;
			case GPDBWritable.TEXT:
			case GPDBWritable.VARCHAR:
				oneField.val = val;
				break;
			default:
				throw new IOException("Unsupported type " + type);
			}
		}

		fields.add(oneField);
	}
}
