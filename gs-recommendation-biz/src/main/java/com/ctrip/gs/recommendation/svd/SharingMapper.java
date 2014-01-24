package com.ctrip.gs.recommendation.svd;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class to be used by {@link MultithreadedSharingMapper}. Offers
 * "global" before() and after() methods that will typically be used to set up
 * static variables.
 * 
 * Suitable for mappers that need large, read-only in-memory data to operate.
 * 
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public abstract class SharingMapper<K1, V1, K2, V2, S> extends Mapper<K1, V1, K2, V2> {

	private static Object SHARED_INSTANCE;

	/**
	 * Called before the multithreaded execution
	 * 
	 * @param context
	 *            mapper's context
	 */
	abstract S createSharedInstance(Context context) throws IOException;

	final void setupSharedInstance(Context context) throws IOException {
		if (SHARED_INSTANCE == null) {
			SHARED_INSTANCE = createSharedInstance(context);
		}
	}

	@SuppressWarnings("unchecked")
	final S getSharedInstance() {
		return (S) SHARED_INSTANCE;
	}

	static void reset() {
		SHARED_INSTANCE = null;
	}
}