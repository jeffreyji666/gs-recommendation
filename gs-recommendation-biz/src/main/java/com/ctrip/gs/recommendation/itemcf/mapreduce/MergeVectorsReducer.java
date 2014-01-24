package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class MergeVectorsReducer extends
    Reducer<WritableComparable<?>,VectorWritable,WritableComparable<?>,VectorWritable> {

  private final VectorWritable result = new VectorWritable();

  @Override
  public void reduce(WritableComparable<?> key, Iterable<VectorWritable> vectors, Context ctx)
    throws IOException, InterruptedException {
    Vector merged = VectorWritable.merge(vectors.iterator()).get();
    result.set(new SequentialAccessSparseVector(merged));
    ctx.write(key, result);
  }
}
