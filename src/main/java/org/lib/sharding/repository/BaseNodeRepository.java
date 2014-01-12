package org.lib.sharding.repository;

import org.jetbrains.annotations.NotNull;
import org.joda.time.LocalDateTime;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.System.currentTimeMillis;

abstract class BaseNodeRepository implements NodeRepository {
	private static final Logger log = LoggerFactory.getLogger(BaseNodeRepository.class);

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

	protected boolean isNodeExpired(NodeInfo nodeInfo) {
		Long lastUpdateTime = nodeInfo.getLastUpdateTime();
		Long requiredUpdateTime =
			currentTimeMillis()
				- (configuration.getHeartbeatDelay() * 2) * 1000;
		// heartbeat time is expired
		boolean nodeExpired =
			null == lastUpdateTime
				|| requiredUpdateTime > lastUpdateTime;

		if (nodeExpired) {
			log.debug(
				"Node({}) is expired: RequiredUpdateTime ({}) > lastUpdatedTime ({})",
				nodeInfo.getNode(),
				new LocalDateTime(requiredUpdateTime),
				new LocalDateTime(lastUpdateTime)
			);
		}

		return nodeExpired;
	}
}
