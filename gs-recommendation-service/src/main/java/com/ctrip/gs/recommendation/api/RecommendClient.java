package com.ctrip.gs.recommendation.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodecFactory;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.scrooge.Option;
import com.twitter.scrooge.Option.Some;
import com.twitter.util.FutureEventListener;

import com.ctrip.gs.recommendation.thrift.RecommendItemList;
import com.ctrip.gs.recommendation.thrift.RecommendParam;
import com.ctrip.gs.recommendation.thrift.RecommendServices;
import com.ctrip.gs.recommendation.thrift.RecommendType;
import com.ctrip.gs.recommendation.thrift.RecommendTypeParam;
import com.ctrip.gs.recommendation.util.Config;

public class RecommendClient {
	private static final Integer port = Config.getInt("recommendation.server.port");

	// IMPORTANT: this determines how many rpc's are sent in at once.
	// If set to 1, you get no parallelism on for this client.
	static Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
			.hosts(new InetSocketAddress(port))
			.codec(new ThriftClientFramedCodecFactory(new ClientId("RecommendClient"))).hostConnectionLimit(100));
	static RecommendServices.FinagledClient recommendClient = new RecommendServices.FinagledClient(client,
			new TBinaryProtocol.Factory(), "RecommendService");

	public static void main(String[] args) {
		RecommendTypeParam type = new RecommendTypeParam(1, RecommendType.HOTEL, 2);
		List<RecommendTypeParam> types = new ArrayList<RecommendTypeParam>();
		types.add(type);
		Option<String> title = new Some<String>("test");
		Option<String> keywords = new Some<String>("test");
		RecommendParam param = new RecommendParam("test", title, keywords, types);

		recommendClient.recommend(param).addEventListener(new FutureEventListener<List<RecommendItemList>>() {
			@Override
			public void onFailure(Throwable cause) {
				System.out.println("Hi call. Failure: " + cause);
			}

			@Override
			public void onSuccess(List<RecommendItemList> value) {
				System.out.println("Hi call. Success: " + value.size());
			}
		});
	}
}