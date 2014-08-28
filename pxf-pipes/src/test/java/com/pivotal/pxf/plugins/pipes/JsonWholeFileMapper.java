package com.pivotal.pxf.plugins.pipes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonWholeFileMapper extends
		Mapper<Object, BytesWritable, Object, Text> {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);
	private Text outvalue = new Text();
	private StringBuilder bldr = new StringBuilder();
	private JsonNode root = null;
	private JsonNode record = null;
	private String created_at, id, text, screen_name, hashtag, lat, lon;
	private static final String DELIMITER = "|";
	private static final String NEWLINE = "\n";

	@Override
	protected void map(Object key, BytesWritable value, Context context)
			throws IOException, InterruptedException {

		root = decodeLineToJsonNode(new String(value.getBytes(), 0,
				value.getLength()));

		Iterator<JsonNode> iter = root.get("root").getElements();
		while (iter.hasNext()) {
			record = iter.next().get("record");

			created_at = getValueOrEmpty(record, "created_at");
			id = getValueOrEmpty(record, "id");
			text = getValueOrEmpty(record, "text");

			try {
				screen_name = record.get("user").get("screen_name")
						.getValueAsText();
			} catch (NullPointerException e) {
				screen_name = "";
			}

			try {
				hashtag = record.get("entities").get("hashtags").get(0)
						.getValueAsText();
			} catch (NullPointerException e) {
				hashtag = "";
			}

			try {
				lat = record.get("coordinates").get("coordinates").get(0)
						.getValueAsText();
			} catch (NullPointerException e) {
				lat = "";
			}

			try {
				lon = record.get("coordinates").get("coordinates").get(1)
						.getValueAsText();
			} catch (NullPointerException e) {
				lon = "";
			}

			bldr.append(created_at);
			bldr.append(DELIMITER);
			bldr.append(id);
			bldr.append(DELIMITER);
			bldr.append(text);
			bldr.append(DELIMITER);
			bldr.append(screen_name);
			bldr.append(DELIMITER);
			bldr.append(hashtag);
			bldr.append(DELIMITER);
			bldr.append(lat);
			bldr.append(DELIMITER);
			bldr.append(lon);
			bldr.append(NEWLINE);
		}

		outvalue.set(bldr.toString().trim());
		context.write(NullWritable.get(), outvalue);
	}

	private String getValueOrEmpty(JsonNode node, String key) {
		try {
			return node.get(key).getValueAsText();
		} catch (Exception e) {
			return "";
		}
	}

	public synchronized JsonNode decodeLineToJsonNode(String line) {

		try {
			return mapper.readTree(line);
		} catch (JsonParseException e) {
			e.printStackTrace();
			return null;
		} catch (JsonMappingException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
