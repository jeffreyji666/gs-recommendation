package com.ctrip.gs.recommendation.util; 

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

/** 
 * @author:  wgji
 * @date：2014年1月3日 上午9:45:55 
 * @comment: 
 */
public class TopElementsQueue extends PriorityQueue<MutableElement> {

  private final int maxSize;

  private static final int SENTINEL_INDEX = Integer.MIN_VALUE;

  public TopElementsQueue(int maxSize) {
    super(maxSize);
    this.maxSize = maxSize;
  }

  public List<MutableElement> getTopElements() {
    List<MutableElement> topElements = Lists.newArrayListWithCapacity(maxSize);
    while (size() > 0) {
      MutableElement top = pop();
      // filter out "sentinel" objects necessary for maintaining an efficient priority queue
      if (top.index() != SENTINEL_INDEX) {
        topElements.add(top);
      }
    }
    Collections.reverse(topElements);
    return topElements;
  }

  @Override
  protected MutableElement getSentinelObject() {
    return new MutableElement(SENTINEL_INDEX, Double.MIN_VALUE);
  }

  @Override
  protected boolean lessThan(MutableElement e1, MutableElement e2) {
    return e1.get() < e2.get();
  }
}