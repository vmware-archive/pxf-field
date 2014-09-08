package com.pivotal.pxf.plugins.pipes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.pivotal.pxf.api.utilities.InputData;

public class PxfPipesUtil {

	public static Configuration conf = new Configuration();

	public static String getResolverMapperCommand(InputData input) {
		return input.getParametersMap().get("X-GP-RESOLVER-MAPPER");
	}

	public static String getAccessorMapperCommand(InputData input) {
		return input.getParametersMap().get("X-GP-ACCESSOR-MAPPER");
	}

	@SuppressWarnings("unchecked")
	public static Mapper<Object, Object, Object, Text> getResolverMapperClass(
			InputData input) throws Exception {
		if (getResolverMapperCommand(input) != null) {
			return (Mapper<Object, Object, Object, Text>) ReflectionUtils
					.newInstance(
							Class.forName(getResolverMapperCommand(input)),
							conf);
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static Mapper<Object, Object, Object, Object> getAccessorMapperClass(
			InputData input) throws Exception {
		if (getAccessorMapperCommand(input) != null) {
			return (Mapper<Object, Object, Object, Object>) ReflectionUtils
					.newInstance(
							Class.forName(getAccessorMapperCommand(input)),
							conf);
		} else {
			return null;
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
