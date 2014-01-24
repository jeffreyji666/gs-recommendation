package com.ctrip.gs.recommendation.itemcf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.util.PriorityQueue;
import com.google.common.io.Closeables;

public final class UserVectorSplitterMapper extends Mapper<VarLongWritable, VectorWritable, VarIntWritable, VectorOrPrefWritable> {
	private static final Logger logger = LoggerFactory.getLogger(UserVectorSplitterMapper.class);

	public static final String USERS_FILE = "usersFile";
	public static final String MAX_PREFS_PER_USER_CONSIDERED = "maxPrefsPerUserConsidered";
	public static final int DEFAULT_MAX_PREFS_PER_USER_CONSIDERED = 10;

	private int maxPrefsPerUserConsidered;
	private FastIDSet usersToRecommendFor;

	private final VarIntWritable itemIndexWritable = new VarIntWritable();
	private final VectorOrPrefWritable vectorOrPref = new VectorOrPrefWritable();

	@Override
	protected void setup(Context context) throws IOException {
		Configuration jobConf = context.getConfiguration();
		maxPrefsPerUserConsidered = jobConf.getInt(MAX_PREFS_PER_USER_CONSIDERED, DEFAULT_MAX_PREFS_PER_USER_CONSIDERED);
		String usersFilePathString = jobConf.get(USERS_FILE);
		if (usersFilePathString != null) {
			FSDataInputStream in = null;
			try {
				Path unqualifiedUsersFilePath = new Path(usersFilePathString);
				FileSystem fs = FileSystem.get(unqualifiedUsersFilePath.toUri(), jobConf);
				usersToRecommendFor = new FastIDSet();
				Path usersFilePath = unqualifiedUsersFilePath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
				in = fs.open(usersFilePath);
				for (String line : new FileLineIterable(in)) {
					try {
						usersToRecommendFor.add(Long.parseLong(line));
					} catch (NumberFormatException nfe) {
						logger.warn("usersFile line ignored: {}", line);
					}
				}
			} finally {
				Closeables.close(in, true);
			}
		}
	}

	@Override
	protected void map(VarLongWritable key, VectorWritable value, Context context) throws IOException, InterruptedException {
		long userID = key.get();
		if (usersToRecommendFor != null && !usersToRecommendFor.contains(userID)) {
			return;
		}
		Vector userVector = maybePruneUserVector(value.get());

		for (Element e : userVector.nonZeroes()) {
			itemIndexWritable.set(e.index());
			vectorOrPref.set(userID, (float) e.get());
			context.write(itemIndexWritable, vectorOrPref);
		}
	}

	private Vector maybePruneUserVector(Vector userVector) {
		if (userVector.getNumNondefaultElements() <= maxPrefsPerUserConsidered) {
			return userVector;
		}

		float smallestLargeValue = findSmallestLargeValue(userVector);

		// "Blank out" small-sized prefs to reduce the amount of partial
		// products
		// generated later. They're not zeroed, but NaN-ed, so they come through
		// and can be used to exclude these items from prefs.
		for (Element e : userVector.nonZeroes()) {
			float absValue = Math.abs((float) e.get());
			if (absValue < smallestLargeValue) {
				e.set(Float.NaN);
			}
		}

		return userVector;
	}

	private float findSmallestLargeValue(Vector userVector) {
		PriorityQueue<Float> topPrefValues = new PriorityQueue<Float>(maxPrefsPerUserConsidered) {
			@Override
			protected boolean lessThan(Float f1, Float f2) {
				return f1 < f2;
			}
		};

		for (Element e : userVector.nonZeroes()) {
			float absValue = Math.abs((float) e.get());
			topPrefValues.insertWithOverflow(absValue);
		}
		return topPrefValues.top();
	}
}