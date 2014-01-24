package com.ctrip.gs.recommendation.dal;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.gs.recommendation.dal.account.UserBindDao;

/**
 * @author: wgji
 * @date：2014年1月16日 下午2:25:26
 * @comment:
 */
public class UserBindDaoTest extends AbstractSpringTest {
	@Resource
	private UserBindDao userBindDao;

	@Before
	public void setUp() throws SQLException {
	}

	@Test
	public void testUserBind() {
		List<String> bindUserNames = new ArrayList<String>();
		bindUserNames.add("qyyj");
		bindUserNames.add("qyyk");
		bindUserNames.add("qyylyy");
		bindUserNames.add("qyyqq");
		bindUserNames.add("qyyst");

		Map<String, Long> userIds = userBindDao.getUserIds(bindUserNames);
		Assert.assertEquals(bindUserNames.size(), userIds.keySet().size());
	}
}