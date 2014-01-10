package org.lib.sharding.configuration;

import org.lib.sharding.domain.Node;
import org.lib.sharding.domain.ServerNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static java.lang.Integer.parseInt;

@Configuration
@PropertySource("classpath:application.properties")
public class NodeRepositoryConfiguration {

	@Inject
	private Environment env;

	public Integer getHeartbeatDelay() {
		return env.getProperty("sharding.heartbeat.delay", Integer.class);
	}

	public String getShardingKeyspace() {
		return env.getProperty("sharding.cassandra.keyspace");
	}

	public Node getSelfNode() {
		Node selfNode = new ServerNode();
		selfNode.setId(env.getProperty("sharding.node.id", Long.class));
		selfNode.setUrl(env.getProperty("sharding.node.url"));
		return selfNode;
	}
}
