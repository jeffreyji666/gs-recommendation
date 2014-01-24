package com.ctrip.gs.recommendation.util;

import java.util.Collections;
import java.util.List;

import org.apache.mahout.cf.taste.hadoop.MutableRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import com.google.common.collect.Lists;

public class TopItemsQueue extends PriorityQueue<MutableRecommendedItem> {
	private static final long SENTINEL_ID = Long.MIN_VALUE;

	private final int maxSize;

	public TopItemsQueue(int maxSize) {
		super(maxSize);
		this.maxSize = maxSize;
	}

	public List<RecommendedItem> getTopItems() {
		List<RecommendedItem> recommendedItems = Lists.newArrayListWithCapacity(maxSize);
		while (size() > 0) {
			MutableRecommendedItem topItem = pop();
			// filter out "sentinel" objects necessary for maintaining an
			// efficient priority queue
			if (topItem.getItemID() != SENTINEL_ID) {
				recommendedItems.add(topItem);
			}
		}
		Collections.reverse(recommendedItems);
		return recommendedItems;
	}

	@Override
	protected boolean lessThan(MutableRecommendedItem one, MutableRecommendedItem two) {
		return one.getValue() < two.getValue();
	}

	@Override
	protected MutableRecommendedItem getSentinelObject() {
		return new MutableRecommendedItem(SENTINEL_ID, Float.MIN_VALUE);
	}
}