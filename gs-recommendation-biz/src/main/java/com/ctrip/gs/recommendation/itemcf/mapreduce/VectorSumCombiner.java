package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

public class VectorSumCombiner
      extends Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, VectorWritable> {

    private final VectorWritable result = new VectorWritable();

    @Override
    protected void reduce(WritableComparable<?> key, Iterable<VectorWritable> values, Context ctx)
      throws IOException, InterruptedException {
      result.set(Vectors.sum(values.iterator()));
      ctx.write(key, result);
    }
  }