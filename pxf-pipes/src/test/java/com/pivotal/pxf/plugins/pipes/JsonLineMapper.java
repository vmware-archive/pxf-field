package com.pivotal.pxf.plugins.pipes;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonLineMapper extends Mapper<Object, Text, Object, Text> {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);
	private Text outvalue = new Text();
	private StringBuilder bldr = new StringBuilder();
	private JsonNode record = null;
	private String created_at, id, text, screen_name, hashtag, lat, lon;
	private static final String DELIMITER = "|";
	
	public JsonLineMapper() {
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		record = decodeLineToJsonNode(value.toString());

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
		bldr.setLength(0);
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

		outvalue.set(bldr.toString());
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
