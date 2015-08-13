package com.pivotal.pxf.plugins.pipes;

import org.apache.hadoop.conf.Configuration;
import com.pivotal.pxf.api.utilities.InputData;

/**
 * Some utilities and configuration parameters for PXF pipes
 */
public class PxfPipesUtil {

	public static Configuration conf = new Configuration();

	public static String getMapperCommand(InputData input) {
		return input.getUserProperty("MAPPER");
	}

	public static boolean isLineByLine(InputData input) {
		if (input.getUserProperty("LINEBYLINE") == null
				|| !input.getUserProperty("LINEBYLINE")
						.toLowerCase().equals("false")) {
			return true;
		} else {
			return false;
		}
	}

	public static int getQueueSize(InputData input) {
		if (input.getUserProperty("QUEUESIZE") != null) {
			return Integer.parseInt(input.getUserProperty("QUEUESIZE"));
		} else {
			return -1;
		}
	}

	public static byte[] getKeyValueDelimiter(InputData input) {
		if (input.getUserProperty("KEYVALUEDELIMITER") != null) {
			return input.getUserProperty("KEYVALUEDELIMITER")
					.getBytes();
		} else {
			return "\t".getBytes();
		}
	}
}
