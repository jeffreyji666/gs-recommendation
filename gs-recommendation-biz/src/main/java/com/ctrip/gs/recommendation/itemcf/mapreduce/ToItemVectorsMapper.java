package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ToItemVectorsMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, VectorWritable> {
	private final IntWritable itemID = new IntWritable();
	private final VectorWritable itemVectorWritable = new VectorWritable();

	@Override
	protected void map(VarLongWritable rowIndex, VectorWritable vectorWritable, Context ctx) throws IOException,
			InterruptedException {

		Vector userRatings = vectorWritable.get();
		int column = TasteHadoopUtils.idToIndex(rowIndex.get());
		itemVectorWritable.setWritesLaxPrecision(true);

		Vector itemVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		for (Vector.Element elem : userRatings.nonZeroes()) {
			itemID.set(elem.index());
			itemVector.setQuick(column, elem.get());
			itemVectorWritable.set(itemVector);
			ctx.write(itemID, itemVectorWritable);
			// reset vector for reuse
			itemVector.setQuick(elem.index(), 0.0);
		}
	}

}
