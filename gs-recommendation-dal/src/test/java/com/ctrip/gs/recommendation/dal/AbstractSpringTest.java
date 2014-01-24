package com.ctrip.gs.recommendation.dal;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * @author wgji
 */
@ContextConfiguration(locations = "classpath*:applicationContext.dal.test.xml")
public abstract class AbstractSpringTest extends AbstractJUnit4SpringContextTests {

}
