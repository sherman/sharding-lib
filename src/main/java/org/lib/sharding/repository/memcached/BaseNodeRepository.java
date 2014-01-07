package org.lib.sharding.repository.memcached;


import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import net.spy.memcached.CASMutation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.LocalDateTime;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;
import org.lib.sharding.memcached.MemcachedClient;
import org.lib.sharding.repository.NodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;

public abstract class BaseNodeRepository implements NodeRepository {
	private static final Logger log = LoggerFactory.getLogger(BaseNodeRepository.class);
	private static final int MEMCACHED_DELAY_SECONDS = 2;

	@Inject
	private MemcachedClient memcached;

	@Inject
	protected NodeRepositoryConfiguration configuration;

	protected abstract String getNodeCollectionKey();

	private Listener eventListener;

	// local cached value used by Listener
	private volatile Map<Integer, Node> currentNodes = newHashMap();

	@Override
	public void setListener(@NotNull Listener eventListener) {
		this.eventListener = eventListener;
	}

	/**
	 * This method is atomic
	 */
	@Override
	public void heartbeat(final Node node) {
		// if current thread is lucky, initial map will be added
		Map<Integer, NodeInfo> initial = newHashMap();
		addNode(initial, node);

		final ValueHolder<Map<Integer, Node>> newNodes = new ValueHolder<Map<Integer, Node>>();
		newNodes.value = copyOf(toNodeMap(initial));

		newNodes.value = copyOf(
			toNodeMap(
				memcached.cas(
					getNodeCollectionKey(), THIRTY_DAYS, initial,
					new CASMutation<Map<Integer, NodeInfo>>() {
						@Override
						public Map<Integer, NodeInfo> getNewValue(Map<Integer, NodeInfo> current) {
							// check heartbeat expiration
							Set<Node> mustBeRemoved = newHashSet();
							for (Map.Entry<Integer, NodeInfo> nodeElt : current.entrySet()) {
								Long lastUpdateTime = nodeElt.getValue().getLastUpdateTime();
								Long requiredUpdateTime =
									currentTimeMillis()
										- (configuration.getHeartbeatDelay() * 2) * 1000;
								// heartbeat time is expired
								if (
									null == lastUpdateTime
										|| requiredUpdateTime > lastUpdateTime
									) {
									log.debug(
										"Node({}): RequiredUpdateTime ({}) > lastUpdatedTime ([})",
										nodeElt.getValue().getNode(),
										new LocalDateTime(requiredUpdateTime),
										new LocalDateTime(lastUpdateTime)
									);
									mustBeRemoved.add(nodeElt.getValue().getNode());
								}
							}

							for (Node removableNode : mustBeRemoved) {
								removeNode(current, removableNode);
							}

							addNode(current, node);
							return current;
						}
					}
				)
			)
		);

		if (null != eventListener) {
			eventListener.onChange(currentNodes, newNodes.value);
		}

		// update current nodes value in the local cache
		currentNodes = newNodes.value;
	}

	@Override
	@NotNull
	public Map<Integer, Node> getNodes() {
		Map<Integer, NodeInfo> infoMap = getNodeInfoCollection();
		return toNodeMap(infoMap);
	}

	/**
	 * This method is atomic
	 */
	@Override
	public void add(@NotNull final Node node) {
		// if current thread is lucky, initial map will be added
		Map<Integer, NodeInfo> initial = Maps.newHashMap();
		addNode(initial, node);

		memcached.cas(
			getNodeCollectionKey(), THIRTY_DAYS, initial,
			new CASMutation<Map<Integer, NodeInfo>>() {
				@Override
				public Map<Integer, NodeInfo> getNewValue(Map<Integer, NodeInfo> current) {
					addNode(current, node);
					return current;
				}
			}
		);
	}

	/**
	 * This method is atomic
	 */
	@Override
	public void remove(@NotNull final Node node) {
		memcached.cas(
			getNodeCollectionKey(), THIRTY_DAYS, Maps.<Integer, NodeInfo>newHashMap(),
			new CASMutation<Map<Integer, NodeInfo>>() {
				@Override
				public Map<Integer, NodeInfo> getNewValue(Map<Integer, NodeInfo> current) {
					removeNode(current, node);
					return current;
				}
			}
		);
	}

	@Override
	public int size() {
		return getNodeInfoCollection().size();
	}

	@Override
	public void removeAll() {
		memcached.delete(getNodeCollectionKey());
		currentNodes = newHashMap();
	}

	private Map<Integer, NodeInfo> getNodeInfoCollection() {
		Map<Integer, NodeInfo> nodes = memcached.<HashMap<Integer, NodeInfo>>get(getNodeCollectionKey());
		if (null == nodes) {
			nodes = newHashMap();
		}
		return nodes;
	}

	private static void addNode(Map<Integer, NodeInfo> nodes, Node node) {
		NodeInfo updatable = new NodeInfo();
		updatable.setLastUpdateTime(currentTimeMillis());
		updatable.setNode(node);
		nodes.put(getSlotByNode(nodes, node), updatable);
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

	private static void removeNode(Map<Integer, NodeInfo> nodes, Node removableNode) {
		for (Map.Entry<Integer, NodeInfo> node : nodes.entrySet()) {
			if (removableNode.equals(node.getValue().getNode())) {
				nodes.remove(node.getKey());

				if (!node.getKey().equals(nodes.size())) {
					int actualNodeSize = nodes.size();
					nodes.put(node.getKey(), nodes.get(actualNodeSize));
					nodes.remove(actualNodeSize);
				}
				return;
			}
		}
	}

	private static class NodeInfo implements Serializable {
		private Node node;
		private long lastUpdateTime;

		public Node getNode() {
			return node;
		}

		public void setNode(Node node) {
			this.node = node;
		}

		public long getLastUpdateTime() {
			return lastUpdateTime;
		}

		public void setLastUpdateTime(long lastUpdateTime) {
			this.lastUpdateTime = lastUpdateTime;
		}

		@Override
		public boolean equals(Object object) {
			if (this == object) {
				return true;
			}
			if (null == object) {
				return false;
			}

			if (!(object instanceof NodeInfo)) {
				return false;
			}

			NodeInfo o = (NodeInfo) object;

			return equal(node, o.node);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(node);
		}

		@Override
		public String toString() {
			return toStringHelper(this)
				.addValue(node)
				.addValue(lastUpdateTime)
				.toString();
		}
	}

	private class ValueHolder<T> {
		T value;
	}

	private static Map<Integer, Node> toNodeMap(Map<Integer, NodeInfo> nodeInfoMap) {
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
}
