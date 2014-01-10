package org.lib.sharding.configuration;

import com.datastax.driver.core.Cluster;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("test")
public class ClusterConfiguration {

	@Bean
	public Cluster createCluster() {
		Cluster cluster = Cluster
			.builder()
			.withClusterName("test")
			.addContactPoint("127.0.0.1")
			.addContactPoint("127.0.0.2")
			.addContactPoint("127.0.0.3")
			.build();

		return cluster;
	}
}
