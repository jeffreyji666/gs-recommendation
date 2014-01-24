package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

public class ToItemVectorsReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
	private final VectorWritable merged = new VectorWritable();

	@Override
	protected void reduce(IntWritable row, Iterable<VectorWritable> vectors, Context ctx) throws IOException,
			InterruptedException {
		merged.setWritesLaxPrecision(true);
		merged.set(VectorWritable.mergeToVector(vectors.iterator()));

		ctx.write(row, merged);
	}
}
