package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TransposeMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {

  @Override
  protected void map(IntWritable r, VectorWritable v, Context ctx) throws IOException, InterruptedException {
    int row = r.get();
    for (Vector.Element e : v.get().nonZeroes()) {
      RandomAccessSparseVector tmp = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
      tmp.setQuick(row, e.get());
      r.set(e.index());
      ctx.write(r, new VectorWritable(tmp));
    }
  }
}
