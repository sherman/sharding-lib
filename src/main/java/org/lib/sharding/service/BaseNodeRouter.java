package org.lib.sharding.service;

import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.Node;
import org.lib.sharding.repository.NodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.hash.Hashing.consistentHash;
import static com.google.common.hash.Hashing.md5;

abstract class BaseNodeRouter<T> implements NodeRouter<T> {
	private static final Logger log = LoggerFactory.getLogger(BaseNodeRouter.class);

	abstract protected NodeRepository getNodeRepository();

	@Override
	public Node getNodeByKey(@NotNull T elt) {
		Map<Integer, Node> nodes = getNodeRepository().getNodes();
		if (nodes.isEmpty()) {
			throw new IllegalStateException("Nodes list is empty!");
		}

		Node node = nodes.get(consistentHash(md5().hashString(elt.toString()), nodes.size()));
		if (null == node) {
			throw new IllegalStateException("Can't find node for the given key:" + elt.toString());
		}

		return node;
	}

	@Override
	public int getNodesCount() {
		return getNodeRepository().size();
	}

	@Override
	public void heartbeat(@NotNull Node node) {
		getNodeRepository().heartbeat(node);
	}

	@Override
	public Set<Node> getNodes() {
		return copyOf(getNodeRepository().getNodes().values());
	}
}
