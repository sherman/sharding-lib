package org.lib.sharding.repository;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.LocalDateTime;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
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

	protected static void removeNode(Map<Integer, NodeInfo> nodes, Node removableNode) {
		for (Map.Entry<Integer, NodeInfo> node : nodes.entrySet()) {
			if (removableNode.equals(node.getValue().getNode())) {
				nodes.remove(node.getKey());

				if (!node.getKey().equals(nodes.size())) {
					log.debug("Delete node {}", removableNode);
					int actualNodeSize = nodes.size();
					nodes.put(node.getKey(), nodes.get(actualNodeSize));
					nodes.remove(actualNodeSize);
				}
				return;
			}
		}
	}

	protected static void addNode(Map<Integer, NodeInfo> nodes, Node node) {
		NodeInfo updatable = new NodeInfo();
		updatable.setLastUpdateTime(currentTimeMillis());
		updatable.setNode(node);
		nodes.put(getSlotByNode(nodes, node), updatable);
	}

	protected static Map<Integer, Node> toNodeMap(Map<Integer, NodeInfo> nodeInfoMap) {
		return transformValues(
			nodeInfoMap,
			new Function<NodeInfo, Node>() {
				@Override
				public Node apply(@Nullable NodeInfo nodeInfo) {
					assert null != nodeInfo;
					return nodeInfo.getNode();
				}
			}
		);
	}

	private static int getSlotByNode(Map<Integer, NodeInfo> nodes, final Node node) {
		Optional<Map.Entry<Integer, NodeInfo>> found = from(nodes.entrySet()).firstMatch(
			new Predicate<Map.Entry<Integer, NodeInfo>>() {
				@Override
				public boolean apply(@Nullable Map.Entry<Integer, NodeInfo> entry) {
					assert null != entry;
					return entry.getValue().getNode().equals(node);
				}
			}
		);

		if (found.isPresent()) {
			return found.get().getKey();
		} else {
			return nodes.size();
		}
	}
}
