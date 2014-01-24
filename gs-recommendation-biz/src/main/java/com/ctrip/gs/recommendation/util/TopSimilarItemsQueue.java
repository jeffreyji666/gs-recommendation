package com.ctrip.gs.recommendation.util;

import java.util.Collections;
import java.util.List;

import org.apache.mahout.cf.taste.similarity.precompute.SimilarItem;

import com.google.common.collect.Lists;

public class TopSimilarItemsQueue extends PriorityQueue<SimilarItem> {

	private static final long SENTINEL_ID = Long.MIN_VALUE;

	private final int maxSize;

	public TopSimilarItemsQueue(int maxSize) {
		super(maxSize);
		this.maxSize = maxSize;
	}

	public List<SimilarItem> getTopItems() {
		List<SimilarItem> items = Lists.newArrayListWithCapacity(maxSize);
		while (size() > 0) {
			SimilarItem topItem = pop();
			// filter out "sentinel" objects necessary for maintaining an
			// efficient priority queue
			if (topItem.getItemID() != SENTINEL_ID) {
				items.add(topItem);
			}
		}
		Collections.reverse(items);
		return items;
	}

	@Override
	protected boolean lessThan(SimilarItem one, SimilarItem two) {
		return one.getSimilarity() < two.getSimilarity();
	}

	@Override
	protected SimilarItem getSentinelObject() {
		return new SimilarItem(SENTINEL_ID, Double.MIN_VALUE);
	}
}
