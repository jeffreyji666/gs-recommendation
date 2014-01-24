package com.ctrip.gs.recommendation.api;

import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;

import com.ctrip.gs.recommendation.thrift.RecommendServices;
import com.ctrip.gs.recommendation.util.Config;

public class RecommendServer {
	public static final Logger logger = LoggerFactory.getLogger(RecommendServer.class);

	private static final Integer port = Config.getInt("recommendation.server.port");

	public static void main(String[] args) {
		try {
			RecommendServices.FutureIface processor = new RecommendServiceImpl();
			ServerBuilder.safeBuild(
					new RecommendServices.FinagledService(processor, new TBinaryProtocol.Factory()),
					ServerBuilder.get().name("RecommendService").codec(ThriftServerFramedCodec.get())
							.bindTo(new InetSocketAddress(port)));
			logger.info("Start recommendation server on port:" + port);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					logger.info("shutdown recommendation service,port:" + port);
				}
			});
		} catch (Exception ex) {
			logger.error("Could not initialize service", ex);
		}
	}
}