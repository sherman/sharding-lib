package org.lib.sharding.service;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.Node;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newLinkedHashSet;


public class UniqueCounterListener extends DefaultListener {
	private final Set<MapDifference<Integer, Node>> diff = newLinkedHashSet();

	@Override
	public void onChange(@NotNull Map<Integer, Node> oldNodes, @NotNull Map<Integer, Node> newNodes) {
		diff.add(Maps.difference(oldNodes, newNodes));
		super.onChange(oldNodes, newNodes);
	}

	public Set<MapDifference<Integer, Node>> getDifference() {
		return diff;
	}
}
