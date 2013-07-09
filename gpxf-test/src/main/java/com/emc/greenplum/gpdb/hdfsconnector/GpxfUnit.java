package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;

/**
 * This abstract class contains a number of helpful utilities in developing a
 * GPXF extension for HAWQ. Extend this class and use the various
 * <code>assert</code> methods to check given input against known output.
 */
public abstract class GpxfUnit {

	protected static BaseMetaData meta = null;
	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	/**
	 * Uses the given input directory to run through the GPXF unit testing
	 * framework. Uses the lines in the file for output testing.
	 * 
	 * @param input
	 *            Input records
	 * @param expectedOutput
	 *            File containing output to check
	 * @throws Exception
	 */
	public void assertOutput(Path input, Path expectedOutput) throws Exception {

		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				FileSystem.get(new Configuration()).open(expectedOutput)));

		List<String> outputLines = new ArrayList<String>();

		String line;
		while ((line = rdr.readLine()) != null) {
			outputLines.add(line);
		}

		assertOutput(input, outputLines);
	}

	/**
	 * Uses the given input directory to run through the GPXF unit testing
	 * framework. Uses the lines in the given parameter for output testing.
	 * 
	 * @param input
	 *            Input records
	 * @param expectedOutput
	 *            File containing output to check
	 * @throws Exception
	 */
	public void assertOutput(Path input, List<String> expectedOutput)
			throws Exception {

		setup(input);

		IHdfsFileAccessor accessor = getAccessor();
		IFieldsResolver resolver = getResolver();

		List<String> actualOutput = getAllOutput(accessor, resolver);

		if (expectedOutput.size() != actualOutput.size()) {
			System.err.println("Expected Records: " + expectedOutput.size()
					+ "\tActual Records: " + actualOutput.size());
		}

		Assert.assertFalse("Output did not match expected output",
				compareOutput(expectedOutput, actualOutput));
	}

	/**
	 * Uses the given input directory to run through the GPXF unit testing
	 * framework. Uses the lines in the given parameter for output testing.<br>
	 * <br>
	 * Ignores order of records.
	 * 
	 * @param input
	 *            Input records
	 * @param expectedOutput
	 *            File containing output to check
	 * @throws Exception
	 */
	public void assertUnorderedOutput(Path input, Path expectedOutput)
			throws Exception {
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				FileSystem.get(new Configuration()).open(expectedOutput)));

		List<String> outputLines = new ArrayList<String>();

		String line;
		while ((line = rdr.readLine()) != null) {
			outputLines.add(line);
		}

		assertUnorderedOutput(input, outputLines);
	}

	/**
	 * Uses the given input directory to run through the GPXF unit testing
	 * framework. Uses the lines in the file for output testing.<br>
	 * <br>
	 * Ignores order of records.
	 * 
	 * @param input
	 *            Input records
	 * @param expectedOutput
	 *            File containing output to check
	 * @throws Exception
	 */
	public void assertUnorderedOutput(Path input, List<String> expectedOutput)
			throws Exception {
		setup(input);

		IHdfsFileAccessor accessor = getAccessor();
		IFieldsResolver resolver = getResolver();

		List<String> actualOutput = getAllOutput(accessor, resolver);

		if (expectedOutput.size() != actualOutput.size()) {
			System.err.println("Expected Records: " + expectedOutput.size()
					+ "\tActual Records: " + actualOutput.size());
		}

		Assert.assertFalse("Output did not match expected output",
				compareUnorderedOutput(expectedOutput, actualOutput));
	}

	/**
	 * Writes the output to the given output stream. Comma delimiter.
	 * 
	 * @param input
	 *            The input file
	 * @param output
	 *            The output stream
	 * @throws Exception
	 */
	public void writeOutput(Path input, OutputStream output) throws Exception {

		setup(input);

		IHdfsFileAccessor accessor = getAccessor();
		IFieldsResolver resolver = getResolver();

		for (String line : getAllOutput(accessor, resolver)) {
			output.write((line + "\n").getBytes());
		}

		output.flush();
	}

	/**
	 * Get the class of the implementation of {@link IDataFragmenter} to be
	 * tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends IDataFragmenter> getFragmenterClass();

	/**
	 * Get the class of the implementation of {@link IHdfsFileAccessor} to be
	 * tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends IHdfsFileAccessor> getAccessorClass();

	/**
	 * Get the class of the implementation of {@link IFieldsResolver} to be
	 * tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends IFieldsResolver> getResolverClass();

	/**
	 * Get any extra parameters that are meant to be specified for the "gpxf"
	 * protocol. Note that "X-GP-" is prepended to each parameter name.
	 * 
	 * @return Any extra parameters or null if none.
	 */
	public List<Pair<String, String>> getExtraParams() {
		return null;
	}

	/**
	 * Gets the column definition names and data types. Types are integer values
	 * in {@link GPDBWritable}.
	 * 
	 * @return A list of column definition name value pairs
	 */
	public abstract List<Pair<String, Integer>> getColumnDefinitions();

	/**
	 * Set all necessary parameters for GPXF framework to function. Uses the
	 * given path as a single input split.
	 * 
	 * @param input
	 *            The input path, relative or absolute.
	 * @throws Exception
	 */
	protected void setup(Path input) throws Exception {

		Map<String, String> paramsMap = new HashMap<String, String>();

		// HDMetaData parameters
		paramsMap.put("X-GP-ALIGNMENT", "what");
		paramsMap.put("X-GP-SEGMENT-ID", "1");
		paramsMap.put("X-GP-HAS-FILTER", "0");
		paramsMap.put("X-GP-SEGMENT-COUNT", "1");
		paramsMap.put("X-GP-FRAGMENTER", getFragmenterClass().getName());
		paramsMap.put("X-GP-FORMAT", "GPDBWritable");
		paramsMap.put("X-GP-URL-HOST", "localhost");
		paramsMap.put("X-GP-URL-PORT", "50070");

		paramsMap.put("X-GP-DATA-DIR", input.toString());

		List<Pair<String, Integer>> params = getColumnDefinitions();
		paramsMap.put("X-GP-ATTRS", Integer.toString(params.size()));
		for (int i = 0; i < params.size(); ++i) {
			paramsMap.put("X-GP-ATTR-NAME" + i, params.get(i).first);
			paramsMap.put("X-GP-ATTR-TYPE" + i,
					Integer.toString(params.get(i).second));
		}

		// HDFSMetaData properties
		paramsMap.put("X-GP-ACCESSOR", getAccessorClass().getName());
		paramsMap.put("X-GP-RESOLVER", getResolverClass().getName());

		if (getExtraParams() != null) {
			for (Pair<String, String> param : getExtraParams()) {
				paramsMap.put("X-GP-" + param.first, param.second);
			}
		}

		BaseMetaData baseMeta = new BaseMetaData(paramsMap);

		IDataFragmenter fragmenter = getFragmenter(baseMeta);
		String fragments = fragmenter.GetFragments(input.toString());

		JsonNode root = mapper.readTree(fragments);

		int numFragments = 0;
		Iterator<JsonNode> iter = root.getElements().next().getElements();
		while (iter.hasNext()) {
			++numFragments;
			iter.next();
		}

		StringBuilder bldr = new StringBuilder();
		for (int i = 0; i < numFragments; ++i) {
			bldr.append(i + ",");
		}
		bldr.deleteCharAt(bldr.length() - 1);

		paramsMap.put("X-GP-DATA-FRAGMENTS", bldr.toString());

		meta = new HDFSMetaData(new HDMetaData(paramsMap));

	}

	/**
	 * Compares the expected and actual output, printing out any errors.
	 * 
	 * @param expectedOutput
	 *            The expected output
	 * @param actualOutput
	 *            The actual output
	 * @return True if no errors, false otherwise.
	 */
	protected boolean compareOutput(List<String> expectedOutput,
			List<String> actualOutput) {

		boolean error = false;
		for (int i = 0; i < expectedOutput.size(); ++i) {
			boolean match = false;
			for (int j = 0; j < actualOutput.size(); ++j) {
				if (expectedOutput.get(i).equals(actualOutput.get(j))) {
					match = true;
					if (i != j) {
						System.err.println("Expected (" + expectedOutput.get(i)
								+ ") matched (" + actualOutput.get(j)
								+ ") but in wrong place.  " + j
								+ " instead of " + i);
						error = true;
					}

					break;
				}
			}

			if (!match) {
				System.err.println("Missing expected output: ("
						+ expectedOutput.get(i) + ")");
				error = true;
			}
		}

		for (int i = 0; i < actualOutput.size(); ++i) {
			boolean match = false;
			for (int j = 0; j < expectedOutput.size(); ++j) {
				if (actualOutput.get(i).equals(expectedOutput.get(j))) {
					match = true;
					break;
				}
			}

			if (!match) {
				System.err.println("Received unexpected output: ("
						+ actualOutput.get(i) + ")");
				error = true;
			}
		}

		return error;
	}

	/**
	 * Compares the expected and actual output, printing out any errors.
	 * 
	 * @param expectedOutput
	 *            The expected output
	 * @param actualOutput
	 *            The actual output
	 * @return True if no errors, false otherwise.
	 */
	protected boolean compareUnorderedOutput(List<String> expectedOutput,
			List<String> actualOutput) {

		boolean error = false;
		for (int i = 0; i < expectedOutput.size(); ++i) {
			boolean match = false;
			for (int j = 0; j < actualOutput.size(); ++j) {
				if (expectedOutput.get(i).equals(actualOutput.get(j))) {
					match = true;
					break;
				}
			}

			if (!match) {
				System.err.println("Missing expected output: ("
						+ expectedOutput.get(i) + ")");
				error = true;
			}
		}

		for (int i = 0; i < actualOutput.size(); ++i) {
			boolean match = false;
			for (int j = 0; j < expectedOutput.size(); ++j) {
				if (actualOutput.get(i).equals(expectedOutput.get(j))) {
					match = true;
					break;
				}
			}

			if (!match) {
				System.err.println("Received unexpected output: ("
						+ actualOutput.get(i) + ")");
				error = true;
			}
		}

		return error;
	}

	/**
	 * Opens the accessor and reads all output, giving it to the resolver to
	 * retrieve the list of fields. These fields are then added to a string,
	 * delimited by commas, and returned in a list.
	 * 
	 * @param accessor
	 *            The accessor instance to use
	 * @param resolver
	 *            The resolver instance to use
	 * @return
	 * @throws Exception
	 */
	protected List<String> getAllOutput(IHdfsFileAccessor accessor,
			IFieldsResolver resolver) throws Exception {

		Assert.assertTrue("Accessor failed to open", accessor.Open());

		List<String> output = new ArrayList<String>();

		OneRow row = null;
		while ((row = accessor.LoadNextObject()) != null) {

			StringBuilder bldr = new StringBuilder();
			for (OneField field : resolver.GetFields(row)) {
				bldr.append((field != null && field.val != null ? field.val
						: "") + ",");
			}

			if (bldr.length() > 0) {
				bldr.deleteCharAt(bldr.length() - 1);
			}

			output.add(bldr.toString());
		}

		accessor.Close();

		return output;
	}

	/**
	 * Gets an instance of {@link IDataFragmenter} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some
	 * BaseMetaData type
	 * 
	 * @return A IDataFragmenter instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected IDataFragmenter getFragmenter(BaseMetaData meta) throws Exception {

		IDataFragmenter fragmenter = null;

		for (Constructor<?> c : getFragmenterClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (BaseMetaData.class.isAssignableFrom(clazz)) {
						fragmenter = (IDataFragmenter) c.newInstance(meta);
					}
				}
			}
		}

		if (fragmenter == null) {
			throw new InvalidParameterException(
					"Unable to find Fragmenter constructor with a BaseMetaData parameter");
		}

		return fragmenter;

	}

	/**
	 * Gets an instance of {@link IHdfsFileAccessor} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some
	 * BaseMetaData type
	 * 
	 * @return A IHdfsFileAccessor instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected IHdfsFileAccessor getAccessor() throws Exception {

		IHdfsFileAccessor accessor = null;

		for (Constructor<?> c : getAccessorClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (BaseMetaData.class.isAssignableFrom(clazz)) {
						accessor = (IHdfsFileAccessor) c.newInstance(meta);
					}
				}
			}
		}

		if (accessor == null) {
			throw new InvalidParameterException(
					"Unable to find Accessor constructor with a BaseMetaData parameter");
		}

		return accessor;

	}

	/**
	 * Gets an instance of {@link IFieldsResolver} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some
	 * BaseMetaData type
	 * 
	 * @return A IFieldsResolver instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected IFieldsResolver getResolver() throws Exception {

		IFieldsResolver resolver = null;

		// search for a constructor that has a single parameter of a type of
		// BaseMetaData to create the accessor instance
		for (Constructor<?> c : getResolverClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (BaseMetaData.class.isAssignableFrom(clazz)) {
						resolver = (IFieldsResolver) c.newInstance(meta);
					}
				}
			}
		}

		if (resolver == null) {
			throw new InvalidParameterException(
					"Unable to find Resolver constructor with a BaseMetaData parameter");
		}

		return resolver;
	}

	public static class Pair<FIRST, SECOND> {
	
		public FIRST first;
		public SECOND second;
	
		public Pair() {
		}
	
		public Pair(FIRST f, SECOND s) {
			this.first = f;
			this.second = s;
		}
	}
}
