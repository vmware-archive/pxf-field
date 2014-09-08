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

	public static boolean isLineByLine(InputData input) {
		if (input.getParametersMap().get("X-GP-LINEBYLINE") == null
				|| !input.getParametersMap().get("X-GP-LINEBYLINE")
						.toLowerCase().equals("false")) {
			return true;
		} else {
			return false;
		}
	}
}
