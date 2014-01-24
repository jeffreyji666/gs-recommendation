package com.ctrip.gs.recommendation.svd;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;

/**
 * <p>
 * Split a recommendation dataset into a training and a test set
 * </p>
 * 
 * <p>
 * Command line arguments specific to this class are:
 * </p>
 * <li>--trainingPercentage (double): percentage of the data to use as training
 * set (optional, default 0.9)</li> <li>--probePercentage (double): percentage
 * of the data to use as probe set (optional, default 0.1)</li> </ol>
 */
public class DatasetSplitter extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(DatasetSplitter.class);

	private static final String TRAINING_PERCENTAGE = DatasetSplitter.class.getName() + ".trainingPercentage";
	private static final String PROBE_PERCENTAGE = DatasetSplitter.class.getName() + ".probePercentage";
	private static final String PART_TO_USE = DatasetSplitter.class.getName() + ".partToUse";

	private static final Text INTO_TRAINING_SET = new Text("T");
	private static final Text INTO_PROBE_SET = new Text("P");

	private static final double DEFAULT_TRAINING_PERCENTAGE = 0.9;
	private static final double DEFAULT_PROBE_PERCENTAGE = 0.1;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DatasetSplitter(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (commandLine.hasOption("help")) {
			new HelpFormatter().printHelp("item similarity", options);
			return -1;
		}
		double trainingPercentage = Double.parseDouble(getOptionValue(commandLine, "trainingPercentage",
				String.valueOf(DEFAULT_TRAINING_PERCENTAGE)));
		double probePercentage = Double.parseDouble(getOptionValue(commandLine, "probePercentage",
				String.valueOf(DEFAULT_PROBE_PERCENTAGE)));

		Path markedPrefs = getSvdTempPath("markedPreferences");
		Path trainingSetPath = getSvdOutputPath("trainingSet");
		Path probeSetPath = getSvdOutputPath("probeSet");

		Job markPreferenceJob = new Job(getConf(), "markPreferenceJob");
		markPreferenceJob.setJarByClass(MarkPreferencesMapper.class);
		markPreferenceJob.setMapperClass(MarkPreferencesMapper.class);
		markPreferenceJob.setMapOutputKeyClass(Text.class);
		markPreferenceJob.setMapOutputValueClass(Text.class);
		markPreferenceJob.setInputFormatClass(TextInputFormat.class);
		markPreferenceJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(markPreferenceJob, getInputPath());
		FileOutputFormat.setOutputPath(markPreferenceJob, markedPrefs);
		markPreferenceJob.getConfiguration().set(TRAINING_PERCENTAGE, String.valueOf(trainingPercentage));
		markPreferenceJob.getConfiguration().set(PROBE_PERCENTAGE, String.valueOf(probePercentage));
		boolean succeeded = markPreferenceJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("mark preference job has been finished");

		Job createTrainingSetJob = new Job(getConf(), "createTrainingSetJob");
		createTrainingSetJob.setJarByClass(WritePrefsMapper.class);
		createTrainingSetJob.setMapperClass(WritePrefsMapper.class);
		createTrainingSetJob.setMapOutputKeyClass(NullWritable.class);
		createTrainingSetJob.setMapOutputValueClass(Text.class);
		createTrainingSetJob.setInputFormatClass(SequenceFileInputFormat.class);
		createTrainingSetJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(markPreferenceJob, markedPrefs);
		FileOutputFormat.setOutputPath(markPreferenceJob, trainingSetPath);
		createTrainingSetJob.getConfiguration().set(PART_TO_USE, INTO_TRAINING_SET.toString());
		succeeded = createTrainingSetJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("create training set job has been finished");

		Job createProbeSetJob = new Job(getConf(), "createProbeSetJob");
		createProbeSetJob.setJarByClass(WritePrefsMapper.class);
		createProbeSetJob.setMapperClass(WritePrefsMapper.class);
		createProbeSetJob.setMapOutputKeyClass(NullWritable.class);
		createProbeSetJob.setMapOutputValueClass(Text.class);
		createProbeSetJob.setInputFormatClass(SequenceFileInputFormat.class);
		createProbeSetJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(markPreferenceJob, markedPrefs);
		FileOutputFormat.setOutputPath(markPreferenceJob, probeSetPath);
		createProbeSetJob.getConfiguration().set(PART_TO_USE, INTO_PROBE_SET.toString());
		succeeded = createProbeSetJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}

		return 0;
	}

	static class MarkPreferencesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Random random;
		private double trainingBound;
		private double probeBound;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			random = RandomUtils.getRandom();
			trainingBound = Double.parseDouble(ctx.getConfiguration().get(TRAINING_PERCENTAGE));
			probeBound = trainingBound + Double.parseDouble(ctx.getConfiguration().get(PROBE_PERCENTAGE));
		}

		@Override
		protected void map(LongWritable key, Text text, Context ctx) throws IOException, InterruptedException {
			double randomValue = random.nextDouble();
			if (randomValue <= trainingBound) {
				ctx.write(INTO_TRAINING_SET, text);
			} else if (randomValue <= probeBound) {
				ctx.write(INTO_PROBE_SET, text);
			}
		}
	}

	static class WritePrefsMapper extends Mapper<Text, Text, NullWritable, Text> {
		private String partToUse;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			partToUse = ctx.getConfiguration().get(PART_TO_USE);
		}

		@Override
		protected void map(Text key, Text text, Context ctx) throws IOException, InterruptedException {
			if (partToUse.equals(key.toString())) {
				ctx.write(NullWritable.get(), text);
			}
		}
	}

	@Override
	protected Options initOpions() {
		Option trainingPercentage = buildOption("trainingPercentage", "percentage of the data to use as training set (default: "
				+ DEFAULT_TRAINING_PERCENTAGE + ')', String.valueOf(DEFAULT_TRAINING_PERCENTAGE));
		Option probePercentage = buildOption("probePercentage", "percentage of the data to use as probe set (default: "
				+ DEFAULT_PROBE_PERCENTAGE + ')', String.valueOf(DEFAULT_PROBE_PERCENTAGE));
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(trainingPercentage);
		options.addOption(probePercentage);
		options.addOption(help);

		return options;
	}
}