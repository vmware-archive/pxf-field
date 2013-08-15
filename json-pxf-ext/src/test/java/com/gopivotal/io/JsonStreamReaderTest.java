package com.gopivotal.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonStreamReaderTest {

	private JsonStreamReader rdr = null;

	private static final String FILE = "src/test/resources/sample.json";

	@Before
	public void setup() throws FileNotFoundException {
		rdr = new JsonStreamReader("menuitem", new FileInputStream(FILE));
	}

	@Test
	public void testReadRecords() throws IOException {
		int count = 0;
		String record = null;
		while ((record = rdr.getJsonRecord()) != null) {
			++count;
			System.out.println(record);
		}

		Assert.assertEquals(3, count);
	}
}
