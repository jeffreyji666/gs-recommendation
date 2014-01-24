package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

public class MergeVectorsCombiner
    extends Reducer<WritableComparable<?>,VectorWritable,WritableComparable<?>,VectorWritable> {

  @Override
  public void reduce(WritableComparable<?> key, Iterable<VectorWritable> vectors, Context ctx)
    throws IOException, InterruptedException {
    ctx.write(key, VectorWritable.merge(vectors.iterator()));
  }
}
