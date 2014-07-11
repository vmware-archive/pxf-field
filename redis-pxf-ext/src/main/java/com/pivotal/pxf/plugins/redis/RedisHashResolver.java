package com.pivotal.pxf.plugins.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

/**
 * This Redis resolver for PXF will decode a given object from the
 * {@link RedisHashAccessor} into a row for HAWQ.
 * 
 * It will also write key value pairs to a specific hash.
 */
public class RedisHashResolver extends Plugin implements ReadResolver {

	private ArrayList<OneField> fields = new ArrayList<OneField>();

	public RedisHashResolver(InputData inputData) throws Exception {
		super(inputData);
	}

	@Override
	public List<OneField> getFields(OneRow paramOneRow) throws Exception {
		fields.clear();

		addFieldFromString(
				DataType.valueOf(inputData.getColumn(0).columnTypeName()),
				(String) paramOneRow.getKey());
		addFieldFromString(
				DataType.valueOf(inputData.getColumn(1).columnTypeName()),
				(String) paramOneRow.getData());

		return fields;
	}

	private void addFieldFromString(DataType type, String val)
			throws IOException {
		OneField oneField = new OneField();
		oneField.type = type.getOID();

		if (val == null) {
			oneField.val = null;
		} else {
			switch (type) {
			case BIGINT:
				oneField.val = Long.parseLong(val);
				break;
			case BOOLEAN:
				oneField.val = Boolean.parseBoolean(val);
				break;
			case BPCHAR:
			case CHAR:
				oneField.val = val.charAt(0);
				break;
			case BYTEA:
				oneField.val = val.getBytes();
				break;
			case FLOAT8:
			case REAL:
				oneField.val = Double.parseDouble(val);
				break;
			case INTEGER:
			case SMALLINT:
				oneField.val = Integer.parseInt(val);
				break;
			case TEXT:
			case VARCHAR:
				oneField.val = val;
				break;
			default:
				throw new IOException("Unsupported type " + type);
			}
		}

		fields.add(oneField);
	}
}
