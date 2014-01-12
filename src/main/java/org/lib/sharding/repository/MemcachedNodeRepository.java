package org.lib.sharding.repository;


import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import net.spy.memcached.CASMutation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.LocalDateTime;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Node;
import org.lib.sharding.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;

public class MemcachedNodeRepository extends BaseNodeRepository {
	private static final Logger log = LoggerFactory.getLogger(MemcachedNodeRepository.class);
	private static final int MEMCACHED_DELAY_SECONDS = 2;

	// local cached value used by Listener
	private volatile Map<Integer, Node> currentNodes = newHashMap();

	private final MemcachedClient memcached;

	public MemcachedNodeRepository(String suffix, MemcachedClient memcached, NodeRepositoryConfiguration configuration) {
		super(configuration, MemcachedNodeRepository.class.getName() + suffix);
		this.memcached = memcached;
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
					suffix, THIRTY_DAYS, initial,
					new CASMutation<Map<Integer, NodeInfo>>() {
						@Override
						public Map<Integer, NodeInfo> getNewValue(Map<Integer, NodeInfo> current) {
							// check heartbeat expiration
							Set<Node> mustBeRemoved = newHashSet();
							for (Map.Entry<Integer, NodeInfo> nodeElt : current.entrySet()) {
								if (isNodeExpired(nodeElt.getValue())) {
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
			suffix, THIRTY_DAYS, initial,
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
			suffix, THIRTY_DAYS, Maps.<Integer, NodeInfo>newHashMap(),
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
	public void removeAll() {
		memcached.delete(suffix);
		currentNodes = newHashMap();
	}

	private Map<Integer, NodeInfo> getNodeInfoCollection() {
		Map<Integer, NodeInfo> nodes = memcached.<HashMap<Integer, NodeInfo>>get(suffix);
		if (null == nodes) {
			nodes = newHashMap();
		}
		return nodes;
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
