package org.lib.sharding.domain;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface Listener {
	public void onChange(@NotNull Map<Integer, Node> oldNodes, @NotNull Map<Integer, Node> newNodes);
}
