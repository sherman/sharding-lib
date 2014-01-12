package org.lib.sharding.configuration;

import com.datastax.driver.core.Cluster;
import org.lib.sharding.repository.NodeRepository;
import org.lib.sharding.repository.CassandraNodeRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import javax.inject.Inject;
import javax.inject.Named;

@Configuration
@Import({ClusterConfiguration.class, NodeRepositoryConfiguration.class})
@PropertySource("classpath:application.properties")
public class CassandraShardingConfiguration {

	@Inject
	private Environment env;

	@Inject
	private Cluster cluster;

	@Inject
	private NodeRepositoryConfiguration configuration;

	@Bean
	@Named("client")
	public NodeRepository createClientRepository() {
		NodeRepository repository = new CassandraNodeRepository(
			"client_nodes",
			cluster,
			getKeyspace(),
			configuration
		);
		return repository;
	}

	public String getKeyspace() {
		return env.getProperty("sharding.cassandra.keyspace");
	}
}
