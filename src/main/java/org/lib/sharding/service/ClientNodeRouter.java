package org.lib.sharding.service;

import org.lib.sharding.repository.NodeRepository;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class ClientNodeRouter extends BaseNodeRouter<Long> {

	@Inject
	@Named("client")
	private NodeRepository nodeRepository;

	@Override
	protected NodeRepository getNodeRepository() {
		return nodeRepository;
	}
}
