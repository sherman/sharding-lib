package org.lib.sharding.configuration;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import net.spy.memcached.ConnectionFactoryBuilder;
import org.jetbrains.annotations.Nullable;
import org.lib.sharding.memcached.MemcachedClient;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.FluentIterable.from;
import static net.spy.memcached.ConnectionFactoryBuilder.Locator.CONSISTENT;

@Configuration
@PropertySource("classpath:application.properties")
public class MemcachedClientConfiguration {

	@Inject
	private Environment env;

	@Inject
	private net.spy.memcached.MemcachedClient memcachedClient;

	@Bean
	public net.spy.memcached.MemcachedClient createSpyMemcachedClient() throws IOException {
		ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();
		// FIXME: to config
		connectionFactoryBuilder.setOpTimeout(2000);
		connectionFactoryBuilder.setLocatorType(CONSISTENT);

		return new net.spy.memcached.MemcachedClient(
			connectionFactoryBuilder.build(),
			getServers(env.getProperty("sharding.memcached.servers"))
		);
	}

	// FIXME: extract to standalone configuration
	@Bean
	public MemcachedClient createClient() {
		return new MemcachedClient(memcachedClient);
	}

	private List<InetSocketAddress> getServers(String servers) {
		return from(on(",").split(servers)).transform(
			new Function<String, InetSocketAddress>() {
				@Override
				public InetSocketAddress apply(@Nullable String server) {
					assert null != server;
					String[] server_data = server.split(":");
					return new InetSocketAddress(server_data[0], Integer.parseInt(server_data[1]));
				}
			}
		).toList();
	}
}
