package com.pivotal.pxf.plugins.pipes;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;

public class PxfPipesWholeFileCommandTest extends PxfUnit {

	private static List<Pair<String, DataType>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {

		columnDefs = new ArrayList<Pair<String, DataType>>();

		columnDefs.add(new Pair<String, DataType>("created_at", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("id", DataType.BIGINT));
		columnDefs.add(new Pair<String, DataType>("text", DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("user.screen_name",
				DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("entities.hashtags[0]",
				DataType.TEXT));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[0]",
				DataType.FLOAT8));
		columnDefs.add(new Pair<String, DataType>("coordinates.coordinates[1]",
				DataType.FLOAT8));
	}

	@Before
	public void setup() {
		extraParams.add(new Pair<String, String>("MAPPER", System
				.getProperty("user.dir")
				+ "/src/test/resources/json-wf-mapper.py"));
	}

	@After
	public void cleanup() {
		extraParams.clear();
	}

	@Test
	public void testWellFormedJson() throws Exception {

		extraParams.add(new Pair<String, String>("IDENTIFIER", "record"));
		extraParams.add(new Pair<String, String>("ONERECORDPERLINE", "false"));

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013|343136547115253761|REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. #IRS #DOJ #NSA #tcot|SpreadButter|tweetCongress||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547123646465|@marshafitrie dibagi 1000 aja sha :P|patronusdeadly|||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547136233472|Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS|NoSecrets_Vagas|||");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/tweets-pp.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013|343136547115253761|REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. #IRS #DOJ #NSA #tcot|SpreadButter|tweetCongress||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547123646465|@marshafitrie dibagi 1000 aja sha :P|patronusdeadly|||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547136233472|Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS|NoSecrets_Vagas|||");

		output.add("||||||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547115253761|REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. @GOPoversight @GOPLeader @SenRandPaul @SenTedCruz #tweetCongress #IRS #DOJ #NSA #tcot|SpreadButter|tweetCongress||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547123646465|@marshafitrie dibagi 1000 aja sha :P|patronusdeadly|||\n"
				+ "Fri Jun 07 22:45:02 +0000 2013|343136547136233472|Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS|NoSecrets_Vagas|||");

		super.assertUnorderedOutput(new Path(System.getProperty("user.dir")
				+ "/" + "src/test/resources/tweets-pp*.json"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return WholeFileFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return PipedAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return PipedResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
