package org.lib.sharding.service;

import org.lib.sharding.repository.ClientNodeRepository;
import org.lib.sharding.repository.NodeRepository;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ClientNodeRouter extends BaseNodeRouter<Long> {

	@Inject
	private ClientNodeRepository repository;

	@Override
	protected NodeRepository getNodeRepository() {
		return repository;
	}
}
