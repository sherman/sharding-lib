package org.lib.sharding.service;

import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.Node;

import java.util.Set;

public interface NodeRouter<T> {
	/**
	 * @throws IllegalStateException if there is no nodes
	 * has been found for a given key.
	 */
	public Node getNodeByKey(@NotNull T key);
	public int getNodesCount();
	public void heartbeat(@NotNull Node node);
	public Set<Node> getNodes();
}
