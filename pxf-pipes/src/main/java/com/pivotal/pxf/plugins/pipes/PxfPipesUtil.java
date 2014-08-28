package com.pivotal.pxf.plugins.pipes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.pivotal.pxf.api.utilities.InputData;

public class PxfPipesUtil {

	public static Configuration conf = new Configuration();

	public static String getMapperCommand(InputData input) {
		return input.getParametersMap().get("X-GP-MAPPER");
	}

	@SuppressWarnings("unchecked")
	public static Mapper<Object, Object, Object, Text> getMapperClass(
			InputData input) throws Exception {
		if (getMapperCommand(input) != null) {
			return (Mapper<Object, Object, Object, Text>) ReflectionUtils
					.newInstance(Class.forName(getMapperCommand(input)), conf);
		} else {
			return new Mapper<Object, Object, Object, Text>();
		}
	}

	public static String getDelimiter(InputData input) {
		String del = input.getParametersMap().get("X-GP-DELIMITER");

		if (del == null) {
			del = "\\|";
		}

		return del;
	}
}
