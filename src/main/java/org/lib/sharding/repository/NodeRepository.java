package org.lib.sharding.repository;

import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;

import java.util.Map;

public interface NodeRepository {
	public static final int THIRTY_DAYS = 3600 * 24 * 30;

	public void heartbeat(Node node);
	@NotNull
	public Map<Integer, Node> getNodes();

	public void add(@NotNull Node node);
	public void remove(@NotNull Node node);
	public int size();
	public void removeAll();

	public void setListener(@NotNull Listener eventListener);
}
