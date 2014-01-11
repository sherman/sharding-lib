package org.lib.sharding.configuration;

import org.lib.sharding.memcached.MemcachedClient;
import org.lib.sharding.repository.NodeRepository;
import org.lib.sharding.repository.memcached.SimpleNodeRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import javax.inject.Named;

@Configuration
@Import({NodeRepositoryConfiguration.class, MemcachedClientConfiguration.class})
public class MemcachedShardingConfiguration {
	@Inject
	private NodeRepositoryConfiguration configuration;

	@Inject
	private MemcachedClient memcachedClient;

	@Bean
	@Named("client")
	public NodeRepository createClientRepository() {
		NodeRepository repository = new SimpleNodeRepository("_client_nodes", memcachedClient, configuration);
		return repository;
	}

}
