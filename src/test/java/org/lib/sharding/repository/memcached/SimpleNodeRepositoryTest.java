package org.lib.sharding.repository.memcached;

import com.google.common.collect.ImmutableMap;
import org.lib.sharding.configuration.MemcachedShardingConfiguration;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Node;
import org.lib.sharding.domain.ServerNode;
import org.lib.sharding.memcached.MemcachedClient;
import org.lib.sharding.repository.NodeInfo;
import org.lib.sharding.repository.NodeRepository;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@ContextConfiguration(
	loader = AnnotationConfigContextLoader.class,
	classes = {MemcachedShardingConfiguration.class}
)
@ActiveProfiles("test")
public class SimpleNodeRepositoryTest extends AbstractTestNGSpringContextTests {

	@Inject
	@Named("client")
	private NodeRepository repository;

	@Inject
	private NodeRepositoryConfiguration configuration;

	@Inject
	private MemcachedClient memcachedClient;

	@Test
	public void heartbeat() throws InterruptedException {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		repository.heartbeat(node1);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1));
	}

	@Test
	public void add() {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		repository.add(node1);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1));
	}

	@Test
	public void updateTime() throws InterruptedException {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		repository.heartbeat(node1);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1));

		Map<Integer, NodeInfo> previousNodes = getNodesInfo();

		repository.heartbeat(node1);

		Map<Integer, NodeInfo> nodes = getNodesInfo();
		assertTrue(previousNodes.get(0).getLastUpdateTime() < nodes.get(0).getLastUpdateTime());
	}

	@Test
	public void nodeIsExpired() throws InterruptedException {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		Node node2 = new ServerNode();
		node2.setId(43);
		node2.setUrl("http://43");

		repository.heartbeat(node1);
		repository.heartbeat(node2);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1, 1, node2));

		Thread.sleep(configuration.getHeartbeatDelay() * 2 * 1000 + 1);

		repository.heartbeat(node2);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node2));
	}

	@Test
	public void allNodesAreExpired() throws InterruptedException {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		Node node2 = new ServerNode();
		node2.setId(43);
		node2.setUrl("http://43");

		repository.heartbeat(node1);
		repository.heartbeat(node2);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1, 1, node2));

		Thread.sleep(configuration.getHeartbeatDelay() * 2 * 1000 + 1);

		Node node3 = new ServerNode();
		node3.setId(44);
		node3.setUrl("http://44");

		repository.heartbeat(node3);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node3));
	}

	@Test
	public void removeAll() {
		Node node1 = new ServerNode();
		node1.setId(42);
		node1.setUrl("http://42");

		repository.heartbeat(node1);
		assertEquals(repository.getNodes(), ImmutableMap.of(0, node1));

		repository.removeAll();
		assertEquals(repository.getNodes(), ImmutableMap.of());
	}

	@BeforeMethod
	private void cleanUp() {
		repository.removeAll();
	}

	private Map<Integer, NodeInfo> getNodesInfo() {
		// FIXME
		Map<Integer, NodeInfo> nodes = memcachedClient.get(SimpleNodeRepository.class.getName() + "_client_nodes");
		return nodes;
	}
}
