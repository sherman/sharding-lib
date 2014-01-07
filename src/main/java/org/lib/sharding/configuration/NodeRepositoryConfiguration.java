package org.lib.sharding.configuration;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class NodeRepositoryConfiguration {
	@Inject
	@Named("sharding.heartbeat.delay")
	private int heartbeatDelay;

	public int getHeartbeatDelay() {
		return heartbeatDelay;
	}
}
