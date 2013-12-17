package com.pivotal.pxf;

import java.io.BufferedReader;
import java.io.IOException;
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
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.FragmentsOutput;
import com.pivotal.pxf.fragmenters.FragmentsResponseFormatter;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.utilities.InputData;

/**
 * This abstract class contains a number of helpful utilities in developing a
 * PXF extension for HAWQ. Extend this class and use the various
 * <code>assert</code> methods to check given input against known output.
 */
public abstract class PxfUnit {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	protected static List<InputData> inputs = null;

	/**
	 * Uses the given input directory to run through the PXF unit testing
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
	 * Uses the given input directory to run through the PXF unit testing
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
		List<String> actualOutput = new ArrayList<String>();
		for (InputData data : inputs) {
			IReadAccessor accessor = getReadAccessor(data);
			IReadResolver resolver = getReadResolver(data);

			actualOutput.addAll(getAllOutput(accessor, resolver));

			if (expectedOutput.size() != actualOutput.size()) {
				System.err.println("Expected Records: " + expectedOutput.size()
						+ "\tActual Records: " + actualOutput.size());
			}
		}

		Assert.assertFalse("Output did not match expected output",
				compareOutput(expectedOutput, actualOutput));
	}

	/**
	 * Uses the given input directory to run through the PXF unit testing
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
	 * Uses the given input directory to run through the PXF unit testing
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

		List<String> actualOutput = new ArrayList<String>();
		for (InputData data : inputs) {
			IReadAccessor accessor = getReadAccessor(data);
			IReadResolver resolver = getReadResolver(data);

			actualOutput.addAll(getAllOutput(accessor, resolver));

			if (expectedOutput.size() != actualOutput.size()) {
				System.err.println("Expected Records: " + expectedOutput.size()
						+ "\tActual Records: " + actualOutput.size());
			}
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

		for (InputData data : inputs) {
			IReadAccessor accessor = getReadAccessor(data);
			IReadResolver resolver = getReadResolver(data);

			for (String line : getAllOutput(accessor, resolver)) {
				output.write((line + "\n").getBytes());
			}
		}

		output.flush();
	}

	/**
	 * Get the class of the implementation of {@link Fragmenter} to be tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends Fragmenter> getFragmenterClass();

	/**
	 * Get the class of the implementation of {@link Accessor} to be tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends IReadAccessor> getReadAccessorClass();

	/**
	 * Get the class of the implementation of {@link Resolver} to be tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends IReadResolver> getReadResolverClass();

	/**
	 * Get any extra parameters that are meant to be specified for the "pxf"
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

		// 2.1.0 Properties
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
			paramsMap.put("X-GP-ATTR-TYPENAME" + i,
					getTypeName(params.get(i).second));
			paramsMap.put("X-GP-ATTR-TYPECODE" + i,
					Integer.toString(params.get(i).second));
		}

		// HDFSMetaData properties
		paramsMap.put("X-GP-ACCESSOR", getReadAccessorClass().getName());
		paramsMap.put("X-GP-RESOLVER", getReadResolverClass().getName());

		if (getExtraParams() != null) {
			for (Pair<String, String> param : getExtraParams()) {
				paramsMap.put("X-GP-" + param.first, param.second);
			}
		}

		LocalInputData fragmentInputData = new LocalInputData(paramsMap);

		FragmentsOutput fragments = getFragmenter(fragmentInputData)
				.GetFragments();

		String jsonOutput = FragmentsResponseFormatter.formatResponseString(
				fragments, input.toString());

		inputs = new ArrayList<InputData>();

		JsonNode node = decodeLineToJsonNode(jsonOutput);

		JsonNode fragmentsArray = node.get("PXFFragments");
		int i = 0;
		Iterator<JsonNode> iter = fragmentsArray.getElements();
		while (iter.hasNext()) {
			iter.next();
			paramsMap.put("X-GP-DATA-FRAGMENT", Integer.toString(i++));
			inputs.add(new LocalInputData(paramsMap));
		}
	}

	private JsonNode decodeLineToJsonNode(String line) {

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
	protected List<String> getAllOutput(IReadAccessor accessor,
			IReadResolver resolver) throws Exception {

		Assert.assertTrue("Accessor failed to open", accessor.openForRead());

		List<String> output = new ArrayList<String>();

		OneRow row = null;
		while ((row = accessor.readNextObject()) != null) {

			StringBuilder bldr = new StringBuilder();
			for (OneField field : resolver.getFields(row)) {
				bldr.append((field != null && field.val != null ? field.val
						: "") + ",");
			}

			if (bldr.length() > 0) {
				bldr.deleteCharAt(bldr.length() - 1);
			}

			output.add(bldr.toString());
		}

		accessor.closeForRead();

		return output;
	}

	/**
	 * Gets an instance of {@link Fragmenter} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some
	 * BaseMetaData type
	 * 
	 * @return A Fragmenter instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected Fragmenter getFragmenter(InputData meta) throws Exception {

		Fragmenter fragmenter = null;

		for (Constructor<?> c : getFragmenterClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (InputData.class.isAssignableFrom(clazz)) {
						fragmenter = (Fragmenter) c.newInstance(meta);
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
	 * Gets an instance of {@link IReadAccessor} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some InputData
	 * type
	 * 
	 * @return An IReadAccessor instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected IReadAccessor getReadAccessor(InputData data) throws Exception {

		IReadAccessor accessor = null;

		for (Constructor<?> c : getReadAccessorClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (InputData.class.isAssignableFrom(clazz)) {
						accessor = (IReadAccessor) c.newInstance(data);
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
	protected IReadResolver getReadResolver(InputData data) throws Exception {

		IReadResolver resolver = null;

		// search for a constructor that has a single parameter of a type of
		// BaseMetaData to create the accessor instance
		for (Constructor<?> c : getReadResolverClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (InputData.class.isAssignableFrom(clazz)) {
						resolver = (IReadResolver) c.newInstance(data);
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

	private String getTypeName(int oid) {
		switch (oid) {
		case 16:
			return "BOOLEAN";
		case 17:
			return "BYTEA";
		case 18:
			return "CHAR";
		case 20:
			return "BIGINT";
		case 21:
			return "SMALLINT";
		case 23:
			return "INTEGER";
		case 25:
			return "TEXT";
		case 700:
			return "REAL";
		case 701:
			return "FLOAT8";
		case 1042:
			return "BPCHAR";
		case 1043:
			return "VARCHAR";
		case 1082:
			return "DATE";
		case 1083:
			return "TIME";
		case 1114:
			return "TIMESTAMP";
		case 1700:
			return "NUMERIC";
		}
		return "TEXT";
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

	public static class LocalInputData extends InputData {

		public LocalInputData(InputData copy) {
			super(copy);
			super.path = super.path().substring(1);
		}

		public LocalInputData(Map<String, String> paramsMap) {
			super(paramsMap);
			super.path = super.path().substring(1);
		}
	}
}
