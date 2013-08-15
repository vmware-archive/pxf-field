package com.pivotal.pxf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

import com.pivotal.pxf.accessors.Accessor;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.FragmentsOutput;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.Resolver;
import com.pivotal.pxf.utilities.InputData;

/**
 * This abstract class contains a number of helpful utilities in developing a
 * PXF extension for HAWQ. Extend this class and use the various
 * <code>assert</code> methods to check given input against known output.
 */
public abstract class PxfUnit {

	protected static InputData meta = null;

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

		Accessor accessor = getAccessor();
		Resolver resolver = getResolver();

		List<String> actualOutput = getAllOutput(accessor, resolver);

		if (expectedOutput.size() != actualOutput.size()) {
			System.err.println("Expected Records: " + expectedOutput.size()
					+ "\tActual Records: " + actualOutput.size());
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

		Accessor accessor = getAccessor();
		Resolver resolver = getResolver();

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

		Accessor accessor = getAccessor();
		Resolver resolver = getResolver();

		for (String line : getAllOutput(accessor, resolver)) {
			output.write((line + "\n").getBytes());
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
	public abstract Class<? extends Accessor> getAccessorClass();

	/**
	 * Get the class of the implementation of {@link Resolver} to be tested.
	 * 
	 * @return The class
	 */
	public abstract Class<? extends Resolver> getResolverClass();

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

		meta = new InputData(paramsMap);

		Fragmenter fragmenter = getFragmenter(meta);
		FragmentsOutput fragments = fragmenter.GetFragments();

		int numFragments = fragments.getFragments().size();

		for (int i = 0; i < numFragments; ++i) {
			meta.getDataFragments().add(i);
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
	protected List<String> getAllOutput(Accessor accessor, Resolver resolver)
			throws Exception {

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
	 * Gets an instance of {@link Accessor} via reflection.
	 * 
	 * Searches for a constructor that has a single parameter of some InputData
	 * type
	 * 
	 * @return A Accessor instance
	 * @throws Exception
	 *             If something bad happens
	 */
	protected Accessor getAccessor() throws Exception {

		Accessor accessor = null;

		for (Constructor<?> c : getAccessorClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (InputData.class.isAssignableFrom(clazz)) {
						accessor = (Accessor) c.newInstance(meta);
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
	protected Resolver getResolver() throws Exception {

		Resolver resolver = null;

		// search for a constructor that has a single parameter of a type of
		// BaseMetaData to create the accessor instance
		for (Constructor<?> c : getResolverClass().getConstructors()) {
			System.out.println(c);

			if (c.getParameterTypes().length == 1) {
				for (Class<?> clazz : c.getParameterTypes()) {
					System.out.println(clazz);

					if (InputData.class.isAssignableFrom(clazz)) {
						resolver = (Resolver) c.newInstance(meta);
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
