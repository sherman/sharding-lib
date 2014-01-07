package org.lib.sharding.repository;

import org.lib.sharding.repository.memcached.BaseNodeRepository;

public class EventNodeRepository extends BaseNodeRepository {
	@Override
	protected String getNodeCollectionKey() {
		return configuration.getEventNodesKey();
	}
}
