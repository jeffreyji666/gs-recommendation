package com.ctrip.gs.recommendation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.Constant;

/**
 * @author: wgji
 * @date：2013年12月28日 上午12:29:24
 * @comment:job基础类
 */
public abstract class AbstractJob extends Configured implements Tool {
	protected static final String NUM_USERS = "numUsers.bin";
	protected static final String ITEMID_INDEX = "itemIDIndex";
	protected static final String USER_VECTORS = "userVectors";
	protected static final String RATING_MATRIX = "ratingMatrix";
	protected static final String SIMILARITY_MATRIX = "similarityMatrix";
	protected static final String ITEM_SIMILARITY = "itemSimilarity";
	protected static final int DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM = 100;
	protected static final int DEFAULT_MAX_PREFS = 500;
	protected static final int DEFAULT_MIN_PREFS_PER_USER = 1;

	protected Path inputPath = new Path(Config.getString(Constant.HADOOP_HDSF_INPUT_PATH));
	protected Path itemcfOutputPath = new Path(Config.getString(Constant.HADOOP_HDSF_ITEMCF_OUTPUT_PATH));
	protected Path itemcfTempPath = new Path(Config.getString(Constant.HADOOP_HDSF_ITEMCF_TMP_PATH));
	protected Path svdOutputPath = new Path(Config.getString(Constant.HADOOP_HDSF_SVD_OUTPUT_PATH));
	protected Path svdTempPath = new Path(Config.getString(Constant.HADOOP_HDSF_SVD_TMP_PATH));

	protected Path getInputPath() {
		return inputPath;
	}

	protected Path getItemcfOutputPath() {
		return itemcfOutputPath;
	}

	protected Path getItemcfOutputPath(String path) {
		return new Path(itemcfOutputPath, path);
	}

	protected Path getItemcfTempPath() {
		return itemcfTempPath;
	}

	protected Path getSvdTempPath(String directory) {
		return new Path(itemcfTempPath, directory);
	}

	protected Path getSvdOutputPath() {
		return svdOutputPath;
	}

	protected Path getSvdOutputPath(String path) {
		return new Path(itemcfOutputPath, path);
	}

	protected Path getSvdTempPath() {
		return svdTempPath;
	}

	protected Path getItemcfTempPath(String directory) {
		return new Path(itemcfTempPath, directory);
	}

	protected abstract Options initOpions();

	@SuppressWarnings("static-access")
	protected Option buildOption(String name, String description, String defaultValue) {
		return OptionBuilder.withArgName(name).hasArg().withDescription(description).create(defaultValue);
	}

	protected boolean hasOption(CommandLine commandLine, String optionName) {
		return commandLine.hasOption(optionName);
	}

	protected String getOptionValue(CommandLine commandLine, String optionName, String defaultValue) {
		if (hasOption(commandLine, optionName))
			return commandLine.getOptionValue(optionName);
		return defaultValue;
	}

	@Override
	public Configuration getConf() {
		Configuration conf = super.getConf();
		if (conf == null || conf.get(Constant.HADOOP_HDSF_PATH) == null) {
			conf = new Configuration(true);
			conf.addResource("hdfs-site.xml");
			conf.addResource("yarn-site.xml");
			conf.set(Constant.HADOOP_HDSF_PATH, Config.getString(Constant.HADOOP_HDSF_PATH));

			return conf;
		}
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
	}
}