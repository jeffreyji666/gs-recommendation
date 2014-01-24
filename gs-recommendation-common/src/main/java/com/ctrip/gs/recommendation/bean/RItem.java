package com.ctrip.gs.recommendation.bean;

/**
 * @author: wgji
 * @date：2014年1月23日 下午6:11:32
 * @comment:
 */
public class RItem {
	private int productid;
	private double score;

	/**
	 * @return the productid
	 */
	public int getProductid() {
		return productid;
	}

	/**
	 * @param productid
	 *            the productid to set
	 */
	public void setProductid(int productid) {
		this.productid = productid;
	}

	/**
	 * @return the score
	 */
	public double getScore() {
		return score;
	}

	/**
	 * @param score
	 *            the score to set
	 */
	public void setScore(double score) {
		this.score = score;
	}
}