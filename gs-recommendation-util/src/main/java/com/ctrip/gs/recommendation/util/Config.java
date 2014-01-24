package com.ctrip.gs.recommendation.util;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	public static final Logger logger = LoggerFactory.getLogger(Config.class);

	private static final Properties properties = getProperties("config.properties");

	public static void initialize(String fileName) {
		properties.putAll(getProperties(fileName));
		properties.putAll(getProperties("local/config-local.conf"));
	}

	public static String getString(String key) {
		return properties.getProperty(key);
	}

	public static Integer getInt(String key) {
		return Integer.parseInt(getString(key));
	}

	public static Boolean getBool(String key) {
		return getString(key).toLowerCase() == "true";
	}

	public static Properties getProperties(String filename) {
		Properties prop = new Properties();
		InputStream ins = null;
		try {
			ins = ClassLoader.getSystemResourceAsStream(filename);
			logger.info("load properties: " + ins);
			if (ins != null) {
				prop.load(ins);
			}
		} catch (Exception ex) {
			logger.error("Error while loading file: " + filename, ex);
		} finally {
			try {
				if (ins != null)
					ins.close();
			} catch (Exception ex) {
				logger.error("Error while close inputstream: " + filename, ex);
			}
		}
		return prop;
	}
}
