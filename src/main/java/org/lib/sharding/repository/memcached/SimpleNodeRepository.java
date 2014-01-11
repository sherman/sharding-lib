package org.lib.sharding.repository.memcached;

import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.memcached.MemcachedClient;

public class SimpleNodeRepository extends BaseNodeRepository {
	public SimpleNodeRepository(String suffix, MemcachedClient memcached, NodeRepositoryConfiguration configuration) {
		super(SimpleNodeRepository.class.getName() + suffix, memcached, configuration);
	}
}
