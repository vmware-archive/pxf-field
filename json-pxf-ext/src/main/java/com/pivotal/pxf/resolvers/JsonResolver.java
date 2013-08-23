package com.pivotal.pxf.resolvers;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import com.gopivotal.mapred.input.JsonInputFormat;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.Resolver;
import com.pivotal.pxf.utilities.ColumnDescriptor;
import com.pivotal.pxf.utilities.InputData;

public class JsonResolver extends Resolver {

	private ArrayList<OneField> list = new ArrayList<OneField>();
	private InputData meta = null;

	public JsonResolver(InputData meta) throws IOException {
		super(meta);
		this.meta = meta;
	}

	@Override
	public List<OneField> GetFields(OneRow row) throws Exception {
		list.clear();

		// key is a Text object
		JsonNode root = JsonInputFormat.decodeLineToJsonNode(row.getData()
				.toString());

		if (root != null) {
			for (int i = 0; i < meta.columns(); ++i) {
				ColumnDescriptor cd = meta.getColumn(i);

				String[] tokens = cd.columnName().split("\\.");

				JsonNode node = root;
				for (int j = 0; j < tokens.length - 1; ++j) {
					node = node.path(tokens[j]);
				}

				// if the last token is an array index
				if (tokens[tokens.length - 1].contains("[")
						&& tokens[tokens.length - 1].contains("]")) {
					String token = tokens[tokens.length - 1].substring(0,
							tokens[tokens.length - 1].indexOf('['));

					int arrayIndex = Integer.parseInt(tokens[tokens.length - 1]
							.substring(
									tokens[tokens.length - 1].indexOf('[') + 1,
									tokens[tokens.length - 1].length() - 1));

					node = node.get(token);

					if (node == null || node.isMissingNode()) {
						list.add(null);
					} else if (node.isArray()) {
						int count = 0;
						boolean added = false;
						for (Iterator<JsonNode> arrayNodes = node.getElements(); arrayNodes
								.hasNext();) {
							JsonNode arrayNode = arrayNodes.next();

							if (count == arrayIndex) {
								added = true;
								addOneFieldToRecord(list, cd.columnType(),
										arrayNode);
								break;
							}

							++count;
						}

						// if we reached the end of the array without adding a
						// field, add null
						if (!added) {
							list.add(null);
						}

					} else {
						throw new InvalidParameterException(token
								+ " is not an array node");
					}

				} else {
					node = node.get(tokens[tokens.length - 1]);
					if (node == null || node.isMissingNode()) {
						list.add(null);
					} else {
						addOneFieldToRecord(list, cd.columnType(), node);
					}
				}
			}
		}

		return list;
	}

	@SuppressWarnings("deprecation")
	private void addOneFieldToRecord(List<OneField> record,
			int gpdbWritableType, JsonNode val) throws IOException {
		OneField oneField = new OneField();
		oneField.type = gpdbWritableType;
		switch (gpdbWritableType) {
		case GPDBWritable.BIGINT:
			oneField.val = val.getValueAsLong();
			break;
		case GPDBWritable.BOOLEAN:
			oneField.val = val.getValueAsBoolean();
			break;
		case GPDBWritable.BPCHAR:
		case GPDBWritable.CHAR:
			oneField.val = val.getValueAsText().charAt(0);
			break;
		case GPDBWritable.BYTEA:
			oneField.val = val.getValueAsText().getBytes();
			break;
		case GPDBWritable.FLOAT8:
		case GPDBWritable.REAL:
			oneField.val = val.getValueAsDouble();
			break;
		case GPDBWritable.INTEGER:
		case GPDBWritable.SMALLINT:
			oneField.val = val.getValueAsInt();
			break;
		case GPDBWritable.TEXT:
		case GPDBWritable.VARCHAR:
			oneField.val = val.getValueAsText();
			break;
		default:
			throw new IOException("Unsupported type " + gpdbWritableType);
		}

		record.add(oneField);
	}

}
