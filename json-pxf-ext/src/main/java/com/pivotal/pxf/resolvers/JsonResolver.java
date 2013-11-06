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
import com.pivotal.pxf.utilities.ColumnDescriptor;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/**
 * This JSON resolver for PXF will decode a given object from the
 * {@link JsonAccessor} into a row for HAWQ. It will decode this data into a
 * JsonNode and walk the tree for each column. It supports normal value mapping
 * via projections and JSON array indexing.
 */
public class JsonResolver extends Plugin implements IReadResolver {

	private ArrayList<OneField> list = new ArrayList<OneField>();

	public JsonResolver(InputData inputData) throws Exception {
		super(inputData);
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		list.clear();

		// key is a Text object
		JsonNode root = JsonInputFormat.decodeLineToJsonNode(row.getKey()
				.toString());

		// if we weren't given a null object
		if (root != null) {
			// Iterate through the column definition and fetch our JSON data
			for (int i = 0; i < inputData.columns(); ++i) {

				// Get the current column description
				ColumnDescriptor cd = inputData.getColumn(i);
				int columnType = cd.columnTypeCode();

				// Get the JSON projections from the column name
				// For example, "user.name" turns into ["user","name"]
				String[] projs = cd.columnName().split("\\.");

				// Move down the JSON path to the final name
				JsonNode node = getPriorJsonNode(root, projs);

				// If this column is an array index, ex. "tweet.hashtags[0]"
				if (isArrayIndex(projs)) {

					// Get the node name and index
					String nodeName = getArrayName(projs);
					int arrayIndex = getArrayIndex(projs);

					// Move to the array node
					node = node.get(nodeName);

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(columnType);
					} else if (node.isArray()) {
						// If the JSON node is an array, then add it to our list
						addFieldFromJsonArray(columnType, node, arrayIndex);
					} else {
						throw new InvalidParameterException(nodeName
								+ " is not an array node");
					}
				} else {
					// This column is not an array type
					// Move to the final node
					node = node.get(projs[projs.length - 1]);

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(columnType);
					} else {
						// Else, add the value to the record
						addFieldFromJsonNode(columnType, node);
					}
				}
			}
		}

		return list;
	}

	/**
	 * Iterates down the root node to the prior JSON node. This node is used
	 * 
	 * @param root
	 * @param projs
	 * @return
	 */
	private JsonNode getPriorJsonNode(JsonNode root, String[] projs) {

		// Iterate through all the tokens to the desired JSON node
		JsonNode node = root;
		for (int j = 0; j < projs.length - 1; ++j) {
			node = node.path(projs[j]);
		}

		return node;
	}

	/**
	 * Gets a boolean value indicating if this column is an array index column
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private boolean isArrayIndex(String[] projs) {
		return projs[projs.length - 1].contains("[")
				&& projs[projs.length - 1].contains("]");
	}

	/**
	 * Gets the node name from the given String array of JSON projections,
	 * parsed from the ColumnDescriptor's
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @return The name
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private String getArrayName(String[] projs) {
		return projs[projs.length - 1].replaceAll("\\[[0-9]+\\]", "");
	}

	/**
	 * Gets the array index from the given String array of JSON projections,
	 * parsed from the ColumnDescriptor's name
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @return The index
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private int getArrayIndex(String[] projs) {
		return Integer.parseInt(projs[projs.length - 1].substring(
				projs[projs.length - 1].indexOf('[') + 1,
				projs[projs.length - 1].length() - 1));
	}

	/**
	 * Iterates through the given JSON node to the proper index and adds the
	 * field of corresponding type
	 * 
	 * @param type
	 *            The GPDBWritable type
	 * @param node
	 *            The JSON array node
	 * @param index
	 *            The array index to iterate to
	 * @throws IOException
	 */
	private void addFieldFromJsonArray(int type, JsonNode node, int index)
			throws IOException {

		int count = 0;
		boolean added = false;
		for (Iterator<JsonNode> arrayNodes = node.getElements(); arrayNodes
				.hasNext();) {
			JsonNode arrayNode = arrayNodes.next();

			if (count == index) {
				added = true;
				addFieldFromJsonNode(type, arrayNode);
				break;
			}

			++count;
		}

		// if we reached the end of the array without adding a
		// field, add null
		if (!added) {
			addNullField(type);
		}
	}

	/**
	 * Adds a field from a given JSON node value based on the GPDBWritable type.
	 * 
	 * @param type
	 *            The GPDBWritable type
	 * @param val
	 *            The JSON node to extract the value.
	 * @throws IOException
	 */
	private void addFieldFromJsonNode(int type, JsonNode val)
			throws IOException {
		OneField oneField = new OneField();
		oneField.type = type;

		if (val.isNull()) {
			oneField.val = null;
		} else {
			switch (type) {
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
				throw new IOException("Unsupported type " + type);
			}
		}

		list.add(oneField);
	}

	/**
	 * Adds a null field of the given type.
	 * 
	 * @param type
	 *            The GPDBWritable type
	 */
	private void addNullField(int type) {

		list.add(new OneField(type, null));
	}
}
