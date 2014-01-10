package org.lib.sharding.service;

import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.difference;

public class DefaultListener implements Listener {
	private static final Logger log = LoggerFactory.getLogger(DefaultListener.class);

	private static final AtomicInteger idGenerator = new AtomicInteger();

	private final int id;

	public DefaultListener() {
		id = idGenerator.incrementAndGet();
	}

	@Override
	public void onChange(@NotNull Map<Integer, Node> oldNodes, @NotNull Map<Integer, Node> newNodes) {
		log.debug("L{}, Old: {}, New: {}", id, oldNodes, newNodes);
		log.debug("L{}, Node diff: {}", id, difference(oldNodes, newNodes));
	}
}
