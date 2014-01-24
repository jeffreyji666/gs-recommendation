package com.ctrip.gs.recommendation.svd;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.als.MultithreadedSharingMapper;
import org.apache.mahout.cf.taste.hadoop.als.SolveExplicitFeedbackMapper;
import org.apache.mahout.cf.taste.hadoop.als.SolveImplicitFeedbackMapper;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.common.mapreduce.TransposeMapper;
import org.apache.mahout.common.mapreduce.VectorSumCombiner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * <p>
 * MapReduce implementation of the two factorization algorithms described in
 * 
 * <p>
 * "Large-scale Parallel Collaborative Filtering for the Netﬂix Prize" available
 * at http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20
 * Netflix/netflix_aaim08(submitted).pdf.
 * </p>
 * 
 * "
 * <p>
 * Collaborative Filtering for Implicit Feedback Datasets" available at
 * http://research.yahoo.com/pub/2433
 * </p>
 * 
 * </p>
 * <p>
 * Command line arguments specific to this class are:
 * </p>
 * 
 * <ol>
 * <li>--input (path): Directory containing one or more text files with the
 * dataset</li>
 * <li>--output (path): path where output should go</li>
 * <li>--lambda (double): regularization parameter to avoid overfitting</li>
 * <li>--userFeatures (path): path to the user feature matrix</li>
 * <li>--itemFeatures (path): path to the item feature matrix</li>
 * <li>--numThreadsPerSolver (int): threads to use per solver mapper, (default:
 * 1)</li>
 * </ol>
 */
public class ParallelALSFactorizationJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(ParallelALSFactorizationJob.class);

	static final String NUM_FEATURES = ParallelALSFactorizationJob.class.getName() + ".numFeatures";
	static final String LAMBDA = ParallelALSFactorizationJob.class.getName() + ".lambda";
	static final String ALPHA = ParallelALSFactorizationJob.class.getName() + ".alpha";
	static final String NUM_ENTITIES = ParallelALSFactorizationJob.class.getName() + ".numEntities";
	static final String USES_LONG_IDS = ParallelALSFactorizationJob.class.getName() + ".usesLongIDs";
	static final String TOKEN_POS = ParallelALSFactorizationJob.class.getName() + ".tokenPos";

	private boolean implicitFeedback;
	private int numIterations;
	private int numFeatures;
	private double lambda;
	private double alpha;
	private int numThreadsPerSolver;
	private boolean usesLongIDs;
	private int numItems;
	private int numUsers;

	enum Stats {
		NUM_USERS
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ParallelALSFactorizationJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (hasOption(commandLine, "help")) {
			new HelpFormatter().printHelp("prepare preference matrix", options);
			return -1;
		}
		numFeatures = Integer.parseInt(getOptionValue(commandLine, "numFeatures", Boolean.TRUE.toString()));
		numIterations = Integer.parseInt(getOptionValue(commandLine, "numIterations", Boolean.TRUE.toString()));
		lambda = Double.parseDouble(getOptionValue(commandLine, "lambda", Boolean.TRUE.toString()));
		alpha = Double.parseDouble(getOptionValue(commandLine, "alpha", Boolean.TRUE.toString()));
		implicitFeedback = Boolean.parseBoolean(getOptionValue(commandLine, "implicitFeedback", Boolean.TRUE.toString()));
		numThreadsPerSolver = Integer.parseInt(getOptionValue(commandLine, "numThreadsPerSolver", Boolean.TRUE.toString()));
		usesLongIDs = Boolean.parseBoolean(getOptionValue(commandLine, "usesLongIDs", Boolean.TRUE.toString()));

		/*
		 * compute the factorization A = U M'
		 * 
		 * where A (users x items) is the matrix of known ratings U (users x
		 * features) is the representation of users in the feature space M
		 * (items x features) is the representation of items in the feature
		 * space
		 */
		if (usesLongIDs) {
			Job mapUserJob = new Job(getConf(), "mapUserJob");
			mapUserJob.setJarByClass(MapLongIDsMapper.class);
			mapUserJob.setMapperClass(MapLongIDsMapper.class);
			mapUserJob.setMapOutputKeyClass(VarIntWritable.class);
			mapUserJob.setMapOutputValueClass(VarLongWritable.class);
			mapUserJob.setReducerClass(IDMapReducer.class);
			mapUserJob.setOutputKeyClass(VarIntWritable.class);
			mapUserJob.setOutputValueClass(VarLongWritable.class);
			mapUserJob.setInputFormatClass(TextInputFormat.class);
			mapUserJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(mapUserJob, getInputPath());
			FileOutputFormat.setOutputPath(mapUserJob, getSvdOutputPath("userIDIndex"));
			mapUserJob.getConfiguration().set(TOKEN_POS, String.valueOf(TasteHadoopUtils.USER_ID_POS));
			mapUserJob.waitForCompletion(true);

			Job mapItemJob = new Job(getConf(), "mapItemJob");
			mapItemJob.setJarByClass(MapLongIDsMapper.class);
			mapItemJob.setMapperClass(MapLongIDsMapper.class);
			mapItemJob.setMapOutputKeyClass(VarIntWritable.class);
			mapItemJob.setMapOutputValueClass(VarLongWritable.class);
			mapItemJob.setReducerClass(IDMapReducer.class);
			mapItemJob.setOutputKeyClass(VarIntWritable.class);
			mapItemJob.setOutputValueClass(VarLongWritable.class);
			mapItemJob.setInputFormatClass(TextInputFormat.class);
			mapItemJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(mapItemJob, getInputPath());
			FileOutputFormat.setOutputPath(mapItemJob, getSvdOutputPath("itemIDIndex"));
			mapItemJob.getConfiguration().set(TOKEN_POS, String.valueOf(TasteHadoopUtils.ITEM_ID_POS));
			mapItemJob.waitForCompletion(true);
		}

		/* create A' */
		Job itemRatingJob = new Job(getConf(), "itemRatingJob");
		itemRatingJob.setJarByClass(ItemRatingVectorsMapper.class);
		itemRatingJob.setMapperClass(ItemRatingVectorsMapper.class);
		itemRatingJob.setMapOutputKeyClass(IntWritable.class);
		itemRatingJob.setMapOutputValueClass(VectorWritable.class);
		itemRatingJob.setReducerClass(VectorSumReducer.class);
		itemRatingJob.setOutputKeyClass(IntWritable.class);
		itemRatingJob.setOutputValueClass(VectorWritable.class);
		itemRatingJob.setInputFormatClass(TextInputFormat.class);
		itemRatingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(itemRatingJob, getInputPath());
		FileOutputFormat.setOutputPath(itemRatingJob, pathToItemRatings());
		itemRatingJob.setCombinerClass(VectorSumCombiner.class);
		itemRatingJob.getConfiguration().set(USES_LONG_IDS, String.valueOf(usesLongIDs));
		boolean succeeded = itemRatingJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}

		/* create A */
		Job userRatingJob = new Job(getConf(), "userRatingJob");
		userRatingJob.setJarByClass(TransposeMapper.class);
		userRatingJob.setMapperClass(TransposeMapper.class);
		userRatingJob.setMapOutputKeyClass(IntWritable.class);
		userRatingJob.setMapOutputValueClass(VectorWritable.class);
		userRatingJob.setReducerClass(MergeUserVectorsReducer.class);
		userRatingJob.setOutputKeyClass(IntWritable.class);
		userRatingJob.setOutputValueClass(VectorWritable.class);
		userRatingJob.setInputFormatClass(SequenceFileInputFormat.class);
		userRatingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(userRatingJob, pathToItemRatings());
		FileOutputFormat.setOutputPath(userRatingJob, pathToUserRatings());
		userRatingJob.setCombinerClass(MergeVectorsCombiner.class);
		succeeded = userRatingJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}

		// TODO this could be fiddled into one of the upper jobs
		Job averageItemRatingJob = new Job(getConf(), "averageItemRatingJob");
		averageItemRatingJob.setJarByClass(AverageRatingMapper.class);
		averageItemRatingJob.setMapperClass(AverageRatingMapper.class);
		averageItemRatingJob.setMapOutputKeyClass(IntWritable.class);
		averageItemRatingJob.setMapOutputValueClass(VectorWritable.class);
		averageItemRatingJob.setReducerClass(MergeVectorsReducer.class);
		averageItemRatingJob.setOutputKeyClass(IntWritable.class);
		averageItemRatingJob.setOutputValueClass(VectorWritable.class);
		averageItemRatingJob.setInputFormatClass(SequenceFileInputFormat.class);
		averageItemRatingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(averageItemRatingJob, pathToItemRatings());
		FileOutputFormat.setOutputPath(averageItemRatingJob, getSvdTempPath("averageRatings"));
		averageItemRatingJob.setCombinerClass(MergeVectorsCombiner.class);
		succeeded = averageItemRatingJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		Vector averageRatings = ALS.readFirstRow(getItemcfTempPath("averageRatings"), getConf());

		numItems = averageRatings.getNumNondefaultElements();
		numUsers = (int) userRatingJob.getCounters().findCounter(Stats.NUM_USERS).getValue();
		logger.info("Found {} users and {} items", numUsers, numItems);

		/* create an initial M */
		initializeM(averageRatings);

		for (int currentIteration = 0; currentIteration < numIterations; currentIteration++) {
			/* broadcast M, read A row-wise, recompute U row-wise */
			logger.info("Recomputing U (iteration {}/{})", currentIteration, numIterations);
			runSolver(pathToUserRatings(), pathToU(currentIteration), pathToM(currentIteration - 1), currentIteration, "U",
					numItems);
			/* broadcast U, read A' row-wise, recompute M row-wise */
			logger.info("Recomputing M (iteration {}/{})", currentIteration, numIterations);
			runSolver(pathToItemRatings(), pathToM(currentIteration), pathToU(currentIteration), currentIteration, "M", numUsers);
		}

		return 0;
	}

	@SuppressWarnings("deprecation")
	private void initializeM(Vector averageRatings) throws IOException {
		Random random = RandomUtils.getRandom();

		FileSystem fs = FileSystem.get(pathToM(-1).toUri(), getConf());
		SequenceFile.Writer writer = null;
		try {
			writer = new SequenceFile.Writer(fs, getConf(), new Path(pathToM(-1), "part-m-00000"), IntWritable.class,
					VectorWritable.class);

			IntWritable index = new IntWritable();
			VectorWritable featureVector = new VectorWritable();

			for (Vector.Element e : averageRatings.nonZeroes()) {
				Vector row = new DenseVector(numFeatures);
				row.setQuick(0, e.get());
				for (int m = 1; m < numFeatures; m++) {
					row.setQuick(m, random.nextDouble());
				}
				index.set(e.index());
				featureVector.set(row);
				writer.append(index, featureVector);
			}
		} finally {
			Closeables.close(writer, false);
		}
	}

	static class VectorSumReducer extends Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, VectorWritable> {

		private final VectorWritable result = new VectorWritable();

		@Override
		protected void reduce(WritableComparable<?> key, Iterable<VectorWritable> values, Context ctx) throws IOException,
				InterruptedException {
			Vector sum = Vectors.sum(values.iterator());
			result.set(new SequentialAccessSparseVector(sum));
			ctx.write(key, result);
		}
	}

	static class MergeUserVectorsReducer extends
			Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, VectorWritable> {

		private final VectorWritable result = new VectorWritable();

		@Override
		public void reduce(WritableComparable<?> key, Iterable<VectorWritable> vectors, Context ctx) throws IOException,
				InterruptedException {
			Vector merged = VectorWritable.merge(vectors.iterator()).get();
			result.set(new SequentialAccessSparseVector(merged));
			ctx.write(key, result);
			ctx.getCounter(Stats.NUM_USERS).increment(1);
		}
	}

	static class ItemRatingVectorsMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

		private final IntWritable itemIDWritable = new IntWritable();
		private final VectorWritable ratingsWritable = new VectorWritable(true);
		private final Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

		private boolean usesLongIDs;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			usesLongIDs = ctx.getConfiguration().getBoolean(USES_LONG_IDS, false);
		}

		@Override
		protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
			String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
			int userID = TasteHadoopUtils.readID(tokens[TasteHadoopUtils.USER_ID_POS], usesLongIDs);
			int itemID = TasteHadoopUtils.readID(tokens[TasteHadoopUtils.ITEM_ID_POS], usesLongIDs);
			float rating = Float.parseFloat(tokens[2]);

			ratings.setQuick(userID, rating);

			itemIDWritable.set(itemID);
			ratingsWritable.set(ratings);

			ctx.write(itemIDWritable, ratingsWritable);

			// prepare instance for reuse
			ratings.setQuick(userID, 0.0d);
		}
	}

	@SuppressWarnings("deprecation")
	private void runSolver(Path ratings, Path output, Path pathToUorM, int currentIteration, String matrixName, int numEntities)
			throws ClassNotFoundException, IOException, InterruptedException {
		// necessary for local execution in the same JVM only
		SharingMapper.reset();

		Class<? extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>> solverMapperClassInternal;
		String name = null;

		if (implicitFeedback) {
			solverMapperClassInternal = SolveImplicitFeedbackMapper.class;
			name = "Recompute " + matrixName + ", iteration (" + currentIteration + '/' + numIterations + "), " + '('
					+ numThreadsPerSolver + " threads, " + numFeatures + " features, implicit feedback)";
		} else {
			solverMapperClassInternal = SolveExplicitFeedbackMapper.class;
			name = "Recompute " + matrixName + ", iteration (" + currentIteration + '/' + numIterations + "), " + '('
					+ numThreadsPerSolver + " threads, " + numFeatures + " features, explicit feedback)";
		}

		Job solverForUorIJob = new Job(getConf(), "solverForUorIJob");
		solverForUorIJob.setJarByClass(MultithreadedSharingMapper.class);
		solverForUorIJob.setMapperClass(MultithreadedSharingMapper.class);
		solverForUorIJob.setMapOutputKeyClass(IntWritable.class);
		solverForUorIJob.setMapOutputValueClass(VectorWritable.class);
		solverForUorIJob.setInputFormatClass(SequenceFileInputFormat.class);
		solverForUorIJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(solverForUorIJob, ratings);
		FileOutputFormat.setOutputPath(solverForUorIJob, output);
		Configuration solverConf = solverForUorIJob.getConfiguration();
		solverConf.set(LAMBDA, String.valueOf(lambda));
		solverConf.set(ALPHA, String.valueOf(alpha));
		solverConf.setInt(NUM_FEATURES, numFeatures);
		solverConf.set(NUM_ENTITIES, String.valueOf(numEntities));

		FileSystem fs = FileSystem.get(pathToUorM.toUri(), solverConf);
		FileStatus[] parts = fs.listStatus(pathToUorM, PathFilters.partFilter());
		for (FileStatus part : parts) {
			if (logger.isDebugEnabled()) {
				logger.debug("Adding {} to distributed cache", part.getPath().toString());
			}
			DistributedCache.addCacheFile(part.getPath().toUri(), solverConf);
		}
		MultithreadedMapper.setMapperClass(solverForUorIJob, solverMapperClassInternal);
		MultithreadedMapper.setNumberOfThreads(solverForUorIJob, numThreadsPerSolver);
		boolean succeeded = solverForUorIJob.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

	static class AverageRatingMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
		private final IntWritable firstIndex = new IntWritable(0);
		private final Vector featureVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		private final VectorWritable featureVectorWritable = new VectorWritable();

		@Override
		protected void map(IntWritable r, VectorWritable v, Context ctx) throws IOException, InterruptedException {
			RunningAverage avg = new FullRunningAverage();
			for (Vector.Element e : v.get().nonZeroes()) {
				avg.addDatum(e.get());
			}

			featureVector.setQuick(r.get(), avg.getAverage());
			featureVectorWritable.set(featureVector);
			ctx.write(firstIndex, featureVectorWritable);

			// prepare instance for reuse
			featureVector.setQuick(r.get(), 0.0d);
		}
	}

	static class MapLongIDsMapper extends Mapper<LongWritable, Text, VarIntWritable, VarLongWritable> {

		private int tokenPos;
		private final VarIntWritable index = new VarIntWritable();
		private final VarLongWritable idWritable = new VarLongWritable();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			tokenPos = ctx.getConfiguration().getInt(TOKEN_POS, -1);
			Preconditions.checkState(tokenPos >= 0);
		}

		@Override
		protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
			String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());

			long id = Long.parseLong(tokens[tokenPos]);

			index.set(TasteHadoopUtils.idToIndex(id));
			idWritable.set(id);
			ctx.write(index, idWritable);
		}
	}

	static class IDMapReducer extends Reducer<VarIntWritable, VarLongWritable, VarIntWritable, VarLongWritable> {
		@Override
		protected void reduce(VarIntWritable index, Iterable<VarLongWritable> ids, Context ctx) throws IOException,
				InterruptedException {
			ctx.write(index, ids.iterator().next());
		}
	}

	private Path pathToM(int iteration) {
		return iteration == numIterations - 1 ? getItemcfOutputPath("M") : getItemcfTempPath("M-" + iteration);
	}

	private Path pathToU(int iteration) {
		return iteration == numIterations - 1 ? getItemcfOutputPath("U") : getItemcfTempPath("U-" + iteration);
	}

	private Path pathToItemRatings() {
		return getSvdTempPath("itemRatings");
	}

	private Path pathToUserRatings() {
		return getSvdOutputPath("userRatings");
	}

	@Override
	protected Options initOpions() {
		Option lambda = buildOption("lambda", "regularization parameter", Boolean.TRUE.toString());
		Option implicitFeedback = buildOption("implicitFeedback", "data consists of implicit feedback?", Boolean.FALSE.toString());
		Option alpha = buildOption("alpha", "confidence parameter (only used on implicit feedback)", String.valueOf(40));
		Option numFeatures = buildOption("numFeatures", "dimension of the feature space", Boolean.TRUE.toString());
		Option numIterations = buildOption("numIterations", "number of iterations", Boolean.TRUE.toString());
		Option numThreadsPerSolver = buildOption("numThreadsPerSolver", "threads per solver mapper", String.valueOf(1));
		Option usesLongIDs = buildOption("usesLongIDs", "input contains long IDs that need to be translated",
				Boolean.TRUE.toString());
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(lambda);
		options.addOption(implicitFeedback);
		options.addOption(alpha);
		options.addOption(numFeatures);
		options.addOption(numIterations);
		options.addOption(numThreadsPerSolver);
		options.addOption(usesLongIDs);
		options.addOption(help);

		return options;
	}
}