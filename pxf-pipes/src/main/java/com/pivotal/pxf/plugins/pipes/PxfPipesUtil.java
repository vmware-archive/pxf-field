package com.pivotal.pxf.plugins.pipes;

import org.apache.hadoop.conf.Configuration;
import com.pivotal.pxf.api.utilities.InputData;

/**
 * Some utilities and configuration parameters for PXF pipes
 */
public class PxfPipesUtil {

	public static Configuration conf = new Configuration();

	public static String getMapperCommand(InputData input) {
		return input.getProperty("X-GP-MAPPER");
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

	public static int getQueueSize(InputData input) {
		if (input.getParametersMap().get("X-GP-QUEUESIZE") != null) {
			return Integer.parseInt(input.getParametersMap().get(
					"X-GP-QUEUESIZE"));
		} else {
			return -1;
		}
	}

	public static byte[] getKeyValueDelimiter(InputData input) {
		if (input.getParametersMap().get("X-GP-KEYVALUEDELIMITER") != null) {
			return input.getParametersMap().get("X-GP-KEYVALUEDELIMITER")
					.getBytes();
		} else {
			return "\t".getBytes();
		}
	}
}
