package org.lib.sharding.repository;

import org.jetbrains.annotations.NotNull;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public abstract class BaseNodeRepository implements NodeRepository {

	protected final NodeRepositoryConfiguration configuration;
	protected final String suffix;

	// local cached value used by Listener
	protected volatile Map<Integer, Node> currentNodes = newHashMap();

	protected Listener eventListener;

	protected BaseNodeRepository(NodeRepositoryConfiguration configuration, String suffix) {
		this.configuration = configuration;
		this.suffix = suffix;
	}

	@Override
	public void setListener(@NotNull Listener eventListener) {
		this.eventListener = eventListener;
	}

	@Override
	public int size() {
		return getNodes().size();
	}
}
