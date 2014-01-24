package com.ctrip.gs.recommendation.api;

import java.util.List;
import java.util.concurrent.Executors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinRouter;

import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;

import com.ctrip.gs.recommendation.async.UserBehaviorActor;
import com.ctrip.gs.recommendation.thrift.ClickParam;
import com.ctrip.gs.recommendation.thrift.RecommendItemList;
import com.ctrip.gs.recommendation.thrift.RecommendParam;
import com.ctrip.gs.recommendation.thrift.RecommendServices.FutureIface;

public class RecommendServiceImpl implements FutureIface {
	ActorSystem system = ActorSystem.create("GsRecommendSystem");
	ActorRef workerRouter = system.actorOf(Props.create(UserBehaviorActor.class).withRouter(new RoundRobinRouter(10)),
			"workerRouter");

	FuturePool futurePool = new ExecutorServiceFuturePool(Executors.newFixedThreadPool(32));

	@Override
	public Future<List<RecommendItemList>> recommend(RecommendParam param) {
		Function0<List<RecommendItemList>> recommend = new RecommendHandler(param);
		return futurePool.apply(recommend);
	}

	@Override
	public Future<Void> click(ClickParam param) {
		return null;
	}
}