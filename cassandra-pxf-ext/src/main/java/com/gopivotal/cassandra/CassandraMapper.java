package com.gopivotal.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CassandraMapper extends
		Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}

	@Override
	protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value,
			Context context) throws IOException, InterruptedException {
		System.out.println("MAP CALLED");

		String strKey = new String(ByteBufferUtil.string(key));

		for (Entry<ByteBuffer, IColumn> entry : value.entrySet()) {
			String columnName = new String(ByteBufferUtil.string(entry
					.getValue().name()));
			String columnValue = new String(ByteBufferUtil.string(entry
					.getValue().value()));

			context.write(new Text(strKey + "\t" + columnName), new Text(
					columnValue));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
