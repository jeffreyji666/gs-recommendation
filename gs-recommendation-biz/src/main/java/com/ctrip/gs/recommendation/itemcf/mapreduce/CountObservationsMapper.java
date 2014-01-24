package com.ctrip.gs.recommendation.itemcf.mapreduce; 

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/** 
 * @author:  wgji
 * @date：2014年1月2日 下午2:59:45 
 * @comment: 
 */

public class CountObservationsMapper extends Mapper<IntWritable, VectorWritable, NullWritable, VectorWritable> {

	private Vector columnCounts = new RandomAccessSparseVector(Integer.MAX_VALUE);

	@Override
	protected void map(IntWritable rowIndex, VectorWritable rowVectorWritable, Context ctx) throws IOException,
			InterruptedException {

		Vector row = rowVectorWritable.get();
		for (Vector.Element elem : row.nonZeroes()) {
			columnCounts.setQuick(elem.index(), columnCounts.getQuick(elem.index()) + 1);
		}
	}

	@Override
	protected void cleanup(Context ctx) throws IOException, InterruptedException {
		ctx.write(NullWritable.get(), new VectorWritable(columnCounts));
	}
}
