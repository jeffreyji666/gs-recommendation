package com.ctrip.gs.recommendation.dal;

import java.sql.SQLException;
import java.util.Date;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.gs.recommendation.bean.RecommendItem;
import com.ctrip.gs.recommendation.dal.recomm.RecommendItemDao;

public class RecommendItemDaoTest extends AbstractSpringTest {
	@Resource
	private RecommendItemDao recommendItemDao;

	@Before
	public void setUp() throws SQLException {
	}

	@Test
	public void testRecommend() {
		RecommendItem item = new RecommendItem();
		item.setUrl("test");
		item.setName("test");
		item.setDescription("test");
		item.setCreated(new Date());

		recommendItemDao.insertRecommendItem(item);
		int id = item.getId();
		item = recommendItemDao.getRecommendItem(id);
		Assert.assertEquals("test", item.getName());
		Assert.assertEquals("test", item.getUrl());
		Assert.assertEquals("test", item.getDescription());

		item.setName("testUpdated");
		item.setModified(new Date());
		recommendItemDao.updateRecommendItem(item);
		item = recommendItemDao.getRecommendItem(id);
		Assert.assertEquals("testUpdated", item.getName());

		recommendItemDao.deleteRecommendItem(id);
		item = recommendItemDao.getRecommendItem(id);
		Assert.assertNull(item);
	}
}