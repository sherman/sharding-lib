package org.lib.sharding.repository.cassandra;

import com.datastax.driver.core.Cluster;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;

public class SimpleNodeRepository extends BaseNodeRepository {
	public SimpleNodeRepository(String key, Cluster cluster, NodeRepositoryConfiguration configuration) {
		super(key, cluster, configuration);
	}
}
