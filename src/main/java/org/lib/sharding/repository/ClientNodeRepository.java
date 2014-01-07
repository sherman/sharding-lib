package org.lib.sharding.repository;

import org.lib.sharding.repository.memcached.BaseNodeRepository;

import javax.inject.Singleton;

@Singleton
public class ClientNodeRepository extends BaseNodeRepository {
	@Override
	protected String getNodeCollectionKey() {
		return getClass().getName() + "_client_nodes";
	}
}
