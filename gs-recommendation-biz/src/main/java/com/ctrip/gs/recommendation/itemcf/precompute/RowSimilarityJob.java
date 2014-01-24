package com.ctrip.gs.recommendation.itemcf.precompute;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;
import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.hdfs.HdfsSequenceFileWriter;
import com.ctrip.gs.recommendation.itemcf.mapreduce.CountObservationsMapper;
import com.ctrip.gs.recommendation.itemcf.mapreduce.VectorSumCombiner;
import com.ctrip.gs.recommendation.itemcf.mapreduce.VectorSumReducer;
import com.ctrip.gs.recommendation.util.MutableElement;
import com.ctrip.gs.recommendation.util.TopElementsQueue;
import com.ctrip.gs.recommendation.util.Vectors;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

/**
 * @author: wgji
 * @date：2013年12月28日 下午3:09:30
 * @comment:生成相似性矩阵
 */
public class RowSimilarityJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(RowSimilarityJob.class);

	private static final int DEFAULT_MAX_OBSERVATIONS_PER_ROW = 500;
	private static final int DEFAULT_MAX_OBSERVATIONS_PER_COLUMN = 500;
	private static final int DEFAULT_MAX_SIMILARITIES_PER_ROW = 100;
	public static final double NO_THRESHOLD = Double.MIN_VALUE;
	public static final long NO_FIXED_RANDOM_SEED = Long.MIN_VALUE;

	private static final String NUMBER_OF_COLUMNS = RowSimilarityJob.class + ".numberOfColumns";
	private static final String MAX_SIMILARITIES_PER_ROW = RowSimilarityJob.class + ".maxSimilaritiesPerRow";
	private static final String EXCLUDE_SELF_SIMILARITY = RowSimilarityJob.class + ".excludeSelfSimilarity";
	private static final String THRESHOLD = RowSimilarityJob.class + ".threshold";
	private static final String NORMS_PATH = RowSimilarityJob.class + ".normsPath";
	private static final String MAXVALUES_PATH = RowSimilarityJob.class + ".maxWeightsPath";
	private static final String NUM_NON_ZERO_ENTRIES_PATH = RowSimilarityJob.class + ".nonZeroEntriesPath";
	private static final String OBSERVATIONS_PER_COLUMN_PATH = RowSimilarityJob.class + ".observationsPerColumnPath";
	private static final String MAX_OBSERVATIONS_PER_ROW = RowSimilarityJob.class + ".maxObservationsPerRow";
	private static final String MAX_OBSERVATIONS_PER_COLUMN = RowSimilarityJob.class + ".maxObservationsPerColumn";
	private static final String RANDOM_SEED = RowSimilarityJob.class + ".randomSeed";

	private static final int NORM_VECTOR_MARKER = Integer.MIN_VALUE;
	private static final int MAXVALUE_VECTOR_MARKER = Integer.MIN_VALUE + 1;
	private static final int NUM_NON_ZERO_ENTRIES_VECTOR_MARKER = Integer.MIN_VALUE + 2;

	enum Counters {
		ROWS, USED_OBSERVATIONS, NEGLECTED_OBSERVATIONS, COOCCURRENCES, PRUNED_COOCCURRENCES
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RowSimilarityJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (hasOption(commandLine, "help")) {
			new HelpFormatter().printHelp("row similarity", options);
			return -1;
		}
		int maxSimilaritiesPerRow = Integer.parseInt(getOptionValue(commandLine, "maxSimilaritiesPerRow",
				String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ROW)));
		boolean excludeSelfSimilarity = Boolean.parseBoolean(getOptionValue(commandLine, "excludeSelfSimilarity",
				Boolean.TRUE.toString()));
		int maxObservationsPerRow = Integer.parseInt(getOptionValue(commandLine, "maxObservationsPerRow",
				String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_ROW)));
		int maxObservationsPerColumn = Integer.parseInt(getOptionValue(commandLine, "maxObservationsPerColumn",
				String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_COLUMN)));
		double threshold = NO_THRESHOLD;
		long randomSeed = NO_FIXED_RANDOM_SEED;

		Path weightsPath = getItemcfTempPath("weights");
		Path normsPath = getItemcfTempPath("norms.bin");
		Path numNonZeroEntriesPath = getItemcfTempPath("numNonZeroEntries.bin");
		Path maxValuesPath = getItemcfTempPath("maxValues.bin");
		Path pairwiseSimilarityPath = getItemcfTempPath("pairwiseSimilarity");
		Path observationsPerColumnPath = getItemcfTempPath("observationsPerColumn.bin");

		// 删除已经使用的空间
		Configuration conf = getConf();
		HdfsSequenceFileWriter reader = null;
		HdfsUtil.rmr(conf, getItemcfTempPath(SIMILARITY_MATRIX).toString());
		HdfsUtil.rmr(conf, weightsPath.toString());
		HdfsUtil.rmr(conf, normsPath.toString());
		HdfsUtil.rmr(conf, numNonZeroEntriesPath.toString());
		HdfsUtil.rmr(conf, maxValuesPath.toString());
		HdfsUtil.rmr(conf, pairwiseSimilarityPath.toString());
		HdfsUtil.rmr(conf, observationsPerColumnPath.toString());
		HdfsUtil.rmr(conf, getItemcfTempPath("notUsed").toString());

		Job countObservationJob = new Job(getConf(), "countObservationJob");
		countObservationJob.setJarByClass(CountObservationsMapper.class);
		countObservationJob.setMapperClass(CountObservationsMapper.class);
		countObservationJob.setMapOutputKeyClass(NullWritable.class);
		countObservationJob.setMapOutputValueClass(VectorWritable.class);
		countObservationJob.setReducerClass(SumObservationsReducer.class);
		countObservationJob.setOutputKeyClass(NullWritable.class);
		countObservationJob.setOutputValueClass(VectorWritable.class);
		countObservationJob.setInputFormatClass(SequenceFileInputFormat.class);
		countObservationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(countObservationJob, getItemcfTempPath(RATING_MATRIX));
		FileOutputFormat.setOutputPath(countObservationJob, getItemcfTempPath("notUsed"));
		countObservationJob.setCombinerClass(VectorSumCombiner.class);
		countObservationJob.getConfiguration().set(OBSERVATIONS_PER_COLUMN_PATH, observationsPerColumnPath.toString());
		countObservationJob.setNumReduceTasks(1);
		boolean succeeded = countObservationJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("count observation job has been finished");
		System.out.println(Vectors.read(observationsPerColumnPath, conf));

		Job normsAndTransposeJob = new Job(getConf(), "normsAndTransposeJob");
		normsAndTransposeJob.setJarByClass(VectorNormMapper.class);
		normsAndTransposeJob.setMapperClass(VectorNormMapper.class);
		normsAndTransposeJob.setMapOutputKeyClass(IntWritable.class);
		normsAndTransposeJob.setMapOutputValueClass(VectorWritable.class);
		normsAndTransposeJob.setReducerClass(MergeVectorsReducer.class);
		normsAndTransposeJob.setOutputKeyClass(IntWritable.class);
		normsAndTransposeJob.setOutputValueClass(VectorWritable.class);
		normsAndTransposeJob.setCombinerClass(MergeVectorsCombiner.class);
		normsAndTransposeJob.setInputFormatClass(SequenceFileInputFormat.class);
		normsAndTransposeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(normsAndTransposeJob, getItemcfTempPath(RATING_MATRIX));
		FileOutputFormat.setOutputPath(normsAndTransposeJob, weightsPath);

		Configuration normsAndTransposeConf = normsAndTransposeJob.getConfiguration();
		normsAndTransposeConf.set(THRESHOLD, String.valueOf(threshold));
		normsAndTransposeConf.set(NORMS_PATH, normsPath.toString());
		normsAndTransposeConf.set(NUM_NON_ZERO_ENTRIES_PATH, numNonZeroEntriesPath.toString());
		normsAndTransposeConf.set(MAXVALUES_PATH, maxValuesPath.toString());
		normsAndTransposeConf.set(OBSERVATIONS_PER_COLUMN_PATH, observationsPerColumnPath.toString());
		normsAndTransposeConf.set(MAX_OBSERVATIONS_PER_ROW, String.valueOf(maxObservationsPerRow));
		normsAndTransposeConf.set(MAX_OBSERVATIONS_PER_COLUMN, String.valueOf(maxObservationsPerColumn));
		normsAndTransposeConf.set(RANDOM_SEED, String.valueOf(randomSeed));
		succeeded = normsAndTransposeJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("norms and transpose job has been finished");
		reader = new HdfsSequenceFileWriter(conf, weightsPath.toString() + "/part-r-00000", new IntWritable(), new VectorWritable());
		reader.read();

		Job pairwiseSimilarityJob = new Job(getConf(), "pairSimilarityJob");
		pairwiseSimilarityJob.setJarByClass(CooccurrencesMapper.class);
		pairwiseSimilarityJob.setMapperClass(CooccurrencesMapper.class);
		pairwiseSimilarityJob.setMapOutputKeyClass(IntWritable.class);
		pairwiseSimilarityJob.setMapOutputValueClass(VectorWritable.class);
		pairwiseSimilarityJob.setReducerClass(SimilarityReducer.class);
		pairwiseSimilarityJob.setOutputKeyClass(IntWritable.class);
		pairwiseSimilarityJob.setOutputValueClass(VectorWritable.class);
		pairwiseSimilarityJob.setCombinerClass(VectorSumReducer.class);
		pairwiseSimilarityJob.setInputFormatClass(SequenceFileInputFormat.class);
		pairwiseSimilarityJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(pairwiseSimilarityJob, weightsPath);
		FileOutputFormat.setOutputPath(pairwiseSimilarityJob, pairwiseSimilarityPath);

		Configuration pairwiseConf = pairwiseSimilarityJob.getConfiguration();
		pairwiseConf.set(THRESHOLD, String.valueOf(threshold));
		pairwiseConf.set(NORMS_PATH, normsPath.toString());
		pairwiseConf.set(NUM_NON_ZERO_ENTRIES_PATH, numNonZeroEntriesPath.toString());
		pairwiseConf.set(MAXVALUES_PATH, maxValuesPath.toString());
		pairwiseConf.setInt(NUMBER_OF_COLUMNS, HdfsUtil.readInt(getItemcfOutputPath(NUM_USERS).toString(), getConf()));
		pairwiseConf.setBoolean(EXCLUDE_SELF_SIMILARITY, excludeSelfSimilarity);
		succeeded = pairwiseSimilarityJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("pari wise similarity job has been finished");
		reader = new HdfsSequenceFileWriter(conf, pairwiseSimilarityPath.toString() + "/part-r-00000", new IntWritable(),
				new VectorWritable());
		reader.read();

		Job asMatrixJob = new Job(getConf(), "asMatrixJob");
		asMatrixJob.setJarByClass(UnsymmetrifyMapper.class);
		asMatrixJob.setMapperClass(UnsymmetrifyMapper.class);
		asMatrixJob.setMapOutputKeyClass(IntWritable.class);
		asMatrixJob.setMapOutputValueClass(VectorWritable.class);
		asMatrixJob.setReducerClass(MergeToTopKSimilaritiesReducer.class);
		asMatrixJob.setOutputKeyClass(IntWritable.class);
		asMatrixJob.setOutputValueClass(VectorWritable.class);
		asMatrixJob.setCombinerClass(MergeToTopKSimilaritiesReducer.class);
		asMatrixJob.setInputFormatClass(SequenceFileInputFormat.class);
		asMatrixJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(asMatrixJob, pairwiseSimilarityPath);
		FileOutputFormat.setOutputPath(asMatrixJob, getItemcfTempPath(SIMILARITY_MATRIX));
		asMatrixJob.getConfiguration().setInt(MAX_SIMILARITIES_PER_ROW, maxSimilaritiesPerRow);
		succeeded = asMatrixJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("as matrix job has been finished");
		reader = new HdfsSequenceFileWriter(conf, getItemcfTempPath(SIMILARITY_MATRIX).toString() + "/part-r-00000", new IntWritable(),
				new VectorWritable());
		reader.read();

		return 0;
	}

	public static class SumObservationsReducer extends Reducer<NullWritable, VectorWritable, NullWritable, VectorWritable> {
		@Override
		protected void reduce(NullWritable nullWritable, Iterable<VectorWritable> partialVectors, Context ctx)
				throws IOException, InterruptedException {
			Vector counts = Vectors.sum(partialVectors.iterator());
			Vectors.write(counts, new Path(ctx.getConfiguration().get(OBSERVATIONS_PER_COLUMN_PATH)), ctx.getConfiguration());
		}
	}

	public static class VectorNormMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private VectorSimilarityMeasure similarity = new CosineSimilarity();
		private Vector norms;
		private Vector nonZeroEntries;
		private Vector maxValues;
		private double threshold;

		private OpenIntIntHashMap observationsPerColumn;
		private int maxObservationsPerRow;
		private int maxObservationsPerColumn;
		private Random random;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			Configuration conf = ctx.getConfiguration();
			norms = new RandomAccessSparseVector(Integer.MAX_VALUE);
			nonZeroEntries = new RandomAccessSparseVector(Integer.MAX_VALUE);
			maxValues = new RandomAccessSparseVector(Integer.MAX_VALUE);
			threshold = Double.parseDouble(conf.get(THRESHOLD));
			observationsPerColumn = Vectors.readAsIntMap(new Path(conf.get(OBSERVATIONS_PER_COLUMN_PATH)), conf);
			maxObservationsPerRow = conf.getInt(MAX_OBSERVATIONS_PER_ROW, DEFAULT_MAX_OBSERVATIONS_PER_ROW);
			maxObservationsPerColumn = conf.getInt(MAX_OBSERVATIONS_PER_COLUMN, DEFAULT_MAX_OBSERVATIONS_PER_COLUMN);

			long seed = Long.parseLong(conf.get(RANDOM_SEED));
			if (seed == NO_FIXED_RANDOM_SEED) {
				random = RandomUtils.getRandom();
			} else {
				random = RandomUtils.getRandom(seed);
			}
		}

		private Vector sampleDown(Vector rowVector, Context ctx) {
			int observationsPerRow = rowVector.getNumNondefaultElements();
			double rowSampleRate = (double) Math.min(maxObservationsPerRow, observationsPerRow) / (double) observationsPerRow;

			Vector downsampledRow = rowVector.like();
			long usedObservations = 0;
			long neglectedObservations = 0;
			for (Vector.Element elem : rowVector.nonZeroes()) {
				int columnCount = observationsPerColumn.get(elem.index());
				double columnSampleRate = (double) Math.min(maxObservationsPerColumn, columnCount) / (double) columnCount;
				if (random.nextDouble() <= Math.min(rowSampleRate, columnSampleRate)) {
					downsampledRow.setQuick(elem.index(), elem.get());
					usedObservations++;
				} else {
					neglectedObservations++;
				}
			}
			ctx.getCounter(Counters.USED_OBSERVATIONS).increment(usedObservations);
			ctx.getCounter(Counters.NEGLECTED_OBSERVATIONS).increment(neglectedObservations);
			return downsampledRow;
		}

		@Override
		protected void map(IntWritable row, VectorWritable vectorWritable, Context ctx) throws IOException, InterruptedException {
			Vector sampledRowVector = sampleDown(vectorWritable.get(), ctx);
			Vector rowVector = similarity.normalize(sampledRowVector);
			int numNonZeroEntries = 0;
			double maxValue = Double.MIN_VALUE;

			for (Vector.Element element : rowVector.nonZeroes()) {
				RandomAccessSparseVector partialColumnVector = new RandomAccessSparseVector(Integer.MAX_VALUE);
				partialColumnVector.setQuick(row.get(), element.get());
				ctx.write(new IntWritable(element.index()), new VectorWritable(partialColumnVector));
				numNonZeroEntries++;
				if (maxValue < element.get()) {
					maxValue = element.get();
				}
			}
			if (threshold != NO_THRESHOLD) {
				nonZeroEntries.setQuick(row.get(), numNonZeroEntries);
				maxValues.setQuick(row.get(), maxValue);
			}
			norms.setQuick(row.get(), similarity.norm(rowVector));
			ctx.getCounter(Counters.ROWS).increment(1);
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {
			ctx.write(new IntWritable(NORM_VECTOR_MARKER), new VectorWritable(norms));
			ctx.write(new IntWritable(NUM_NON_ZERO_ENTRIES_VECTOR_MARKER), new VectorWritable(nonZeroEntries));
			ctx.write(new IntWritable(MAXVALUE_VECTOR_MARKER), new VectorWritable(maxValues));
		}
	}

	private static class MergeVectorsCombiner extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		@Override
		protected void reduce(IntWritable row, Iterable<VectorWritable> partialVectors, Context ctx) throws IOException,
				InterruptedException {
			ctx.write(row, new VectorWritable(Vectors.merge(partialVectors)));
		}
	}

	public static class MergeVectorsReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private Path normsPath;
		private Path numNonZeroEntriesPath;
		private Path maxValuesPath;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			normsPath = new Path(ctx.getConfiguration().get(NORMS_PATH));
			numNonZeroEntriesPath = new Path(ctx.getConfiguration().get(NUM_NON_ZERO_ENTRIES_PATH));
			maxValuesPath = new Path(ctx.getConfiguration().get(MAXVALUES_PATH));
		}

		@Override
		protected void reduce(IntWritable row, Iterable<VectorWritable> partialVectors, Context ctx) throws IOException,
				InterruptedException {
			Vector partialVector = Vectors.merge(partialVectors);

			if (row.get() == NORM_VECTOR_MARKER) {
				Vectors.write(partialVector, normsPath, ctx.getConfiguration());
			} else if (row.get() == MAXVALUE_VECTOR_MARKER) {
				Vectors.write(partialVector, maxValuesPath, ctx.getConfiguration());
			} else if (row.get() == NUM_NON_ZERO_ENTRIES_VECTOR_MARKER) {
				Vectors.write(partialVector, numNonZeroEntriesPath, ctx.getConfiguration(), true);
			} else {
				ctx.write(row, new VectorWritable(partialVector));
			}
		}
	}

	public static class CooccurrencesMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private VectorSimilarityMeasure similarity = new CosineSimilarity();
		private OpenIntIntHashMap numNonZeroEntries;
		private Vector maxValues;
		private double threshold;

		private static final Comparator<Vector.Element> BY_INDEX = new Comparator<Vector.Element>() {
			@Override
			public int compare(Vector.Element one, Vector.Element two) {
				return Ints.compare(one.index(), two.index());
			}
		};

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			numNonZeroEntries = Vectors.readAsIntMap(new Path(ctx.getConfiguration().get(NUM_NON_ZERO_ENTRIES_PATH)),
					ctx.getConfiguration());
			maxValues = Vectors.read(new Path(ctx.getConfiguration().get(MAXVALUES_PATH)), ctx.getConfiguration());
			threshold = Double.parseDouble(ctx.getConfiguration().get(THRESHOLD));
		}

		private boolean consider(Vector.Element occurrenceA, Vector.Element occurrenceB) {
			int numNonZeroEntriesA = numNonZeroEntries.get(occurrenceA.index());
			int numNonZeroEntriesB = numNonZeroEntries.get(occurrenceB.index());
			double maxValueA = maxValues.get(occurrenceA.index());
			double maxValueB = maxValues.get(occurrenceB.index());

			return similarity.consider(numNonZeroEntriesA, numNonZeroEntriesB, maxValueA, maxValueB, threshold);
		}

		@Override
		protected void map(IntWritable column, VectorWritable occurrenceVector, Context ctx) throws IOException,
				InterruptedException {
			Vector.Element[] occurrences = Vectors.toArray(occurrenceVector);
			Arrays.sort(occurrences, BY_INDEX);

			int cooccurrences = 0;
			int prunedCooccurrences = 0;
			for (int n = 0; n < occurrences.length; n++) {
				Vector.Element occurrenceA = occurrences[n];
				Vector dots = new RandomAccessSparseVector(Integer.MAX_VALUE);
				for (int m = n; m < occurrences.length; m++) {
					Vector.Element occurrenceB = occurrences[m];
					if (threshold == NO_THRESHOLD || consider(occurrenceA, occurrenceB)) {
						dots.setQuick(occurrenceB.index(), similarity.aggregate(occurrenceA.get(), occurrenceB.get()));
						cooccurrences++;
					} else {
						prunedCooccurrences++;
					}
				}
				ctx.write(new IntWritable(occurrenceA.index()), new VectorWritable(dots));
			}
			ctx.getCounter(Counters.COOCCURRENCES).increment(cooccurrences);
			ctx.getCounter(Counters.PRUNED_COOCCURRENCES).increment(prunedCooccurrences);
		}
	}

	public static class SimilarityReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private VectorSimilarityMeasure similarity = new CosineSimilarity();
		private int numberOfColumns;
		private boolean excludeSelfSimilarity;
		private Vector norms;
		private double treshold;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			numberOfColumns = ctx.getConfiguration().getInt(NUMBER_OF_COLUMNS, -1);
			Preconditions.checkArgument(numberOfColumns > 0, "Number of columns must be greater then 0! But numberOfColumns = "
					+ numberOfColumns);
			excludeSelfSimilarity = ctx.getConfiguration().getBoolean(EXCLUDE_SELF_SIMILARITY, false);
			norms = Vectors.read(new Path(ctx.getConfiguration().get(NORMS_PATH)), ctx.getConfiguration());
			treshold = Double.parseDouble(ctx.getConfiguration().get(THRESHOLD));
		}

		@Override
		protected void reduce(IntWritable row, Iterable<VectorWritable> partialDots, Context ctx) throws IOException,
				InterruptedException {
			Iterator<VectorWritable> partialDotsIterator = partialDots.iterator();
			Vector dots = partialDotsIterator.next().get();
			while (partialDotsIterator.hasNext()) {
				Vector toAdd = partialDotsIterator.next().get();
				for (Element nonZeroElement : toAdd.nonZeroes()) {
					dots.setQuick(nonZeroElement.index(), dots.getQuick(nonZeroElement.index()) + nonZeroElement.get());
				}
			}

			Vector similarities = dots.like();
			double normA = norms.getQuick(row.get());
			for (Element b : dots.nonZeroes()) {
				double similarityValue = similarity.similarity(b.get(), normA, norms.getQuick(b.index()), numberOfColumns);
				if (similarityValue >= treshold) {
					similarities.set(b.index(), similarityValue);
				}
			}
			if (excludeSelfSimilarity) {
				similarities.setQuick(row.get(), 0);
			}
			ctx.write(row, new VectorWritable(similarities));
		}
	}

	public static class UnsymmetrifyMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private int maxSimilaritiesPerRow;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			maxSimilaritiesPerRow = ctx.getConfiguration().getInt(MAX_SIMILARITIES_PER_ROW, 0);
			Preconditions.checkArgument(maxSimilaritiesPerRow > 0,
					"Maximum number of similarities per row must be greater then 0!");
		}

		@Override
		protected void map(IntWritable row, VectorWritable similaritiesWritable, Context ctx) throws IOException,
				InterruptedException {
			Vector similarities = similaritiesWritable.get();
			// For performance, the creation of transposedPartial is moved out
			// of the while loop and it is reused inside
			Vector transposedPartial = new RandomAccessSparseVector(similarities.size(), 1);
			TopElementsQueue topKQueue = new TopElementsQueue(maxSimilaritiesPerRow);
			for (Element nonZeroElement : similarities.nonZeroes()) {
				MutableElement top = topKQueue.top();
				double candidateValue = nonZeroElement.get();
				if (candidateValue > top.get()) {
					top.setIndex(nonZeroElement.index());
					top.set(candidateValue);
					topKQueue.updateTop();
				}

				transposedPartial.setQuick(row.get(), candidateValue);
				ctx.write(new IntWritable(nonZeroElement.index()), new VectorWritable(transposedPartial));
				transposedPartial.setQuick(row.get(), 0.0);
			}
			Vector topKSimilarities = new RandomAccessSparseVector(similarities.size(), maxSimilaritiesPerRow);
			for (Vector.Element topKSimilarity : topKQueue.getTopElements()) {
				topKSimilarities.setQuick(topKSimilarity.index(), topKSimilarity.get());
			}
			ctx.write(row, new VectorWritable(topKSimilarities));
		}
	}

	public static class MergeToTopKSimilaritiesReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private int maxSimilaritiesPerRow;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			maxSimilaritiesPerRow = ctx.getConfiguration().getInt(MAX_SIMILARITIES_PER_ROW, 0);
			Preconditions.checkArgument(maxSimilaritiesPerRow > 0,
					"Maximum number of similarities per row must be greater then 0!");
		}

		@Override
		protected void reduce(IntWritable row, Iterable<VectorWritable> partials, Context ctx) throws IOException,
				InterruptedException {
			Vector allSimilarities = Vectors.merge(partials);
			Vector topKSimilarities = Vectors.topKElements(maxSimilaritiesPerRow, allSimilarities);
			ctx.write(row, new VectorWritable(topKSimilarities));
		}
	}

	@Override
	protected Options initOpions() {
		Option maxSimilaritiesPerRow = buildOption("maxSimilaritiesPerRow", "Number of maximum similarities per row (default: "
				+ DEFAULT_MAX_SIMILARITIES_PER_ROW + ')', String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ROW));
		Option excludeSelfSimilarity = buildOption("excludeSelfSimilarity", "compute similarity of rows to themselves?",
				Boolean.TRUE.toString());
		Option maxObservationsPerRow = buildOption("maxObservationsPerRow", "sample rows down to this number of entries",
				String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_ROW));
		Option maxObservationsPerColumn = buildOption("maxObservationsPerColumn",
				"sample columns down to this number of entries", String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_COLUMN));
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(maxSimilaritiesPerRow);
		options.addOption(excludeSelfSimilarity);
		options.addOption(maxObservationsPerRow);
		options.addOption(maxObservationsPerColumn);
		options.addOption(help);

		return options;
	}
}