package com.ctrip.gs.recommendation.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.UntypedActor;

public class UserBehaviorActor extends UntypedActor {
	public static final Logger logger = LoggerFactory.getLogger(UserBehaviorActor.class);

	public void onReceive(Object message) throws Exception {
		if (message instanceof String) {
			logger.info("Received String message: {}", message);
			getSender().tell(message, getSelf());
		} else
			unhandled(message);
	}
}
