package com.pivotal.pxf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.accessors.JsonAccessor;
import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.HdfsDataFragmenter;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.resolvers.JsonResolver;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.PxfUnit;

public class JsonExtensionTest extends PxfUnit {

	private static List<Pair<String, Integer>> columnDefs = null;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {

		columnDefs = new ArrayList<Pair<String, Integer>>();

		columnDefs.add(new Pair<String, Integer>("created_at",
				GPDBWritable.TEXT));
		columnDefs.add(new Pair<String, Integer>("id", GPDBWritable.BIGINT));
		columnDefs.add(new Pair<String, Integer>("text", GPDBWritable.TEXT));
		columnDefs.add(new Pair<String, Integer>("user.screen_name",
				GPDBWritable.TEXT));
		columnDefs.add(new Pair<String, Integer>("entities.hashtags[0]",
				GPDBWritable.TEXT));
		columnDefs.add(new Pair<String, Integer>("coordinates.coordinates[0]",
				GPDBWritable.FLOAT8));
		columnDefs.add(new Pair<String, Integer>("coordinates.coordinates[1]",
				GPDBWritable.FLOAT8));
	}

	@After
	public void cleanup() throws Exception {
		extraParams.clear();
	}

	@Test
	public void testSmallTweets() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");
		output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,It's Jun 7, 2013 @ 11pm ; Wind = NNE (30,0) 14.0 knots; Swell = 2.6 ft @ 5 seconds....,SevenStonesBuoy,,-6.1,50.103");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/tweets-small.json"), output);
	}

	@Test
	public void testTweetsWithNull() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/null-tweets.json"), output);
	}

	@Test
	public void testSmallTweetsWithDelete() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. @GOPoversight @GOPLeader @SenRandPaul @SenTedCruz #tweetCongress #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/tweets-small-with-delete.json"), output);
	}

	@Test
	public void testWellFormedJson() throws Exception {

		extraParams.add(new Pair<String, String>("IDENTIFIER", "record"));
		extraParams.add(new Pair<String, String>("ONERECORDPERLINE", "false"));

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. @GOPoversight @GOPLeader @SenRandPaul @SenTedCruz #tweetCongress #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/tweets-pp.json"), output);
	}

	@Test
	public void testWellFormedJsonWithDelete() throws Exception {

		extraParams.add(new Pair<String, String>("IDENTIFIER", "record"));
		extraParams.add(new Pair<String, String>("ONERECORDPERLINE", "false"));

		List<String> output = new ArrayList<String>();

		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. @GOPoversight @GOPLeader @SenRandPaul @SenTedCruz #tweetCongress #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");

		super.assertOutput(new Path(System.getProperty("user.dir") + "/"
				+ "src/test/resources/tweets-pp-with-delete.json"), output);
	}

	@Test
	public void testMultipleFiles() throws Exception {

		List<String> output = new ArrayList<String>();

		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");
		output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,It's Jun 7, 2013 @ 11pm ; Wind = NNE (30,0) 14.0 knots; Swell = 2.6 ft @ 5 seconds....,SevenStonesBuoy,,-6.1,50.103");
		output.add(",,,,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,REPAIR THE TRUST: REMOVE OBAMA/BIDEN FROM OFFICE. @GOPoversight @GOPLeader @SenRandPaul @SenTedCruz #tweetCongress #IRS #DOJ #NSA #tcot,SpreadButter,tweetCongress,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,@marshafitrie dibagi 1000 aja sha :P,patronusdeadly,,,");
		output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,Vaga: Supervisor de Almoxarifado. Confira em http://t.co/hK5cy5B2oS,NoSecrets_Vagas,,,");

		super.assertUnorderedOutput(new Path(System.getProperty("user.dir")
				+ "/" + "src/test/resources/tweets-small*.json"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return HdfsDataFragmenter.class;
	}

	@Override
	public Class<? extends IReadAccessor> getReadAccessorClass() {
		return JsonAccessor.class;
	}

	@Override
	public Class<? extends IReadResolver> getReadResolverClass() {
		return JsonResolver.class;
	}

	@Override
	public List<Pair<String, Integer>> getColumnDefinitions() {
		return columnDefs;
	}
}
