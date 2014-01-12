package org.lib.sharding.repository;


import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.LocalDateTime;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;

public class CassandraNodeRepository extends BaseNodeRepository {
	private static final Logger log = LoggerFactory.getLogger(CassandraNodeRepository.class);

	private static final int MAX_UPDATE_ATTEMPTS = 0x5;

	private final Cluster cassandraCluster;
	private Session session;

	public CassandraNodeRepository(String suffix, Cluster cassandraCluster, NodeRepositoryConfiguration configuration) {
		super(configuration, suffix);
		this.cassandraCluster = cassandraCluster;

		this.session = cassandraCluster.connect(configuration.getShardingKeyspace());
	}

	@Override
	public synchronized void heartbeat(final Node node) {
		Map<Integer, Node> copyOf = ImmutableMap.copyOf(currentNodes);

		add(node);

		if (null != eventListener) {
			eventListener.onChange(copyOf, currentNodes);
		}
	}

	@Override
	@NotNull
	public Map<Integer, Node> getNodes() {
		NodeCluster cluster = getNodeCluster();
		return toNodeMap(cluster.getNodes());
	}

	@Override
	public synchronized void add(@NotNull final Node node) {
		NodeCluster actual = getNodeCluster();

		long updated = LocalDateTime.now().toDate().getTime();

		NodeInfo nodeInfo = new NodeInfo();
		nodeInfo.setLastUpdateTime(updated);
		nodeInfo.setNode(node);

		if (actual.isEmpty()) {
			Map<Integer, NodeInfo> initial = Maps.newHashMap();
			initial.put(0, nodeInfo);

			NodeCluster initialCluster = new NodeCluster(initial, updated);

			Statement insertStatement = QueryBuilder
				.insertInto("heartbeats_client_nodes1")
				.value("id", "client_nodes")
				.value("nodes", ByteBuffer.wrap(SerializationUtils.serialize(initialCluster.getNodes())))
				.value("updated", initialCluster.getUpdated())
				.ifNotExists();

			log.debug("Insert query is ({})", insertStatement);

			// try insert new
			ResultSet set = session.execute(insertStatement);

			if (set.getAvailableWithoutFetching() > 0 && !set.one().getBool("[applied]")) {
				this.currentNodes = toNodeMap(initialCluster.getNodes());
			} else {
				// someone has been more faster
				update(node);
			}
		} else {
			// nodes already presented
			update(node);
		}
	}

	@Override
	public void remove(@NotNull final Node node) {
		throw new UnsupportedOperationException("Not implemented!");
	}

	@Override
	public void removeAll() {
		session.execute(
			QueryBuilder.delete()
				.from("heartbeats_client_nodes1")
				.where(eq("id", suffix))
		);
		this.currentNodes = newHashMap();
	}

	private void update(Node node) {
		for (int i = 0; i < MAX_UPDATE_ATTEMPTS; i++) {
			NodeCluster cluster = getNodeCluster();
			updateNode(node, cluster);

			removeExpiredNodes(cluster);

			long lastUpdated = LocalDateTime.now().toDate().getTime();

			Statement updateStatement = QueryBuilder.update("heartbeats_client_nodes1")
				.with(set("nodes", ByteBuffer.wrap(SerializationUtils.serialize(cluster.getNodes()))))
				.and(set("updated", lastUpdated))
				.where(eq("id", suffix))
				.onlyIf(eq("updated", cluster.getUpdated()));

			log.debug("Update query is ({})", updateStatement);

			ResultSet set = session.execute(updateStatement);

			if (set.getAvailableWithoutFetching() > 0 && set.one().getBool("[applied]")) {
				cluster.setUpdated(lastUpdated);
				this.currentNodes = toNodeMap(cluster.getNodes());
				return;
			}
		}

		checkArgument(false, "Can't update repository");
	}

	private void removeExpiredNodes(NodeCluster cluster) {
		Set<Node> mustBeRemoved = newHashSet();

		for (NodeInfo nodeInfo : cluster.getNodes().values()) {
			if (isNodeExpired(nodeInfo)) {
				mustBeRemoved.add(nodeInfo.getNode());
			}
		}

		for (Node removableNode : mustBeRemoved) {
			removeNode(cluster.getNodes(), removableNode);
		}
	}

	private static void updateNode(Node node, NodeCluster cluster) {
		for (NodeInfo actualNode : cluster.getNodes().values()) {
			if (actualNode.getNode().equals(node)) {
				actualNode.setLastUpdateTime(LocalDateTime.now().toDate().getTime());
				return;
			}
		}

		NodeInfo nodeInfo = new NodeInfo();
		nodeInfo.setNode(node);
		nodeInfo.setLastUpdateTime(LocalDateTime.now().toDate().getTime());
		// add to the end of the node cluster
		cluster.getNodes().put(cluster.getNodes().size(), nodeInfo);
	}

	private NodeCluster getNodeCluster() {
		ResultSet result = session.execute(
			QueryBuilder
				.select()
				.from("heartbeats_client_nodes1")
				.where(eq("id", suffix))
		);

		Map<Integer, NodeInfo> nodes = newHashMap();
		long updated = 0;

		Row row = result.one();
		if (null != row) {
			try {
				ByteBuffer nodesBuffer = row.getBytes("nodes");
				byte[] nodeBytes = new byte[nodesBuffer.remaining()];
				nodesBuffer.get(nodeBytes);

				nodes = (Map<Integer, NodeInfo>) SerializationUtils.deserialize(nodeBytes);
				updated = row.getLong("updated");
			} catch (Exception e) {
				log.error("Can't get nodes", e);
				nodes = newHashMap();
				updated = 0;
			}
		}

		NodeCluster nodeCluster = new NodeCluster(nodes, updated);
		return nodeCluster;
	}

	private boolean isNodeExpired(NodeInfo nodeInfo) {
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

	private static void removeNode(Map<Integer, NodeInfo> nodes, Node removableNode) {
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

	private static class NodeCluster {
		private final Map<Integer, NodeInfo> nodes;
		private long updated;

		private NodeCluster(Map<Integer, NodeInfo> nodes, long updated) {
			this.nodes = nodes;
			this.updated = updated;
		}

		public Map<Integer, NodeInfo> getNodes() {
			return nodes;
		}

		public long getUpdated() {
			return updated;
		}

		public void setUpdated(long updated) {
			this.updated = updated;
		}

		public boolean isEmpty() {
			return nodes.isEmpty();
		}
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
