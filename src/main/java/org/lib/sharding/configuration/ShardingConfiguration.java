package org.lib.sharding.configuration;

import com.datastax.driver.core.Cluster;
import org.lib.sharding.repository.NodeRepository;
import org.lib.sharding.repository.cassandra.SimpleNodeRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.inject.Inject;
import javax.inject.Named;

@Configuration
@Import({ClusterConfiguration.class, NodeRepositoryConfiguration.class})
public class ShardingConfiguration {

	@Inject
	private Cluster cluster;

	@Inject
	private NodeRepositoryConfiguration configuration;

	@Bean
	@Named("client")
	public NodeRepository createClientRepository() {
		NodeRepository repository = new SimpleNodeRepository("client_nodes", cluster, configuration);
		return repository;
	}
}
