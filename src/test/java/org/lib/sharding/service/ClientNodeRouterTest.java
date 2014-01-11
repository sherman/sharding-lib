package org.lib.sharding.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.lib.sharding.configuration.CassandraShardingConfiguration;
import org.lib.sharding.configuration.ClusterConfiguration;
import org.lib.sharding.configuration.NodeRepositoryConfiguration;
import org.lib.sharding.domain.Listener;
import org.lib.sharding.domain.Node;
import org.lib.sharding.domain.ServerNode;
import org.lib.sharding.repository.NodeRepository;
import org.mockito.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

@ContextConfiguration(
	loader = AnnotationConfigContextLoader.class,
	classes = {ClusterConfiguration.class, CassandraShardingConfiguration.class, ClientNodeRouter.class}
)
@ActiveProfiles("test")
public class ClientNodeRouterTest extends AbstractTestNGSpringContextTests {
	private static final Logger log = LoggerFactory.getLogger(ClientNodeRouterTest.class);

	@Inject
	@Named("client")
	private NodeRepository nodeRepository;

	@Inject
	private ClientNodeRouter nodeRouter;

	@Inject
	private NodeRepositoryConfiguration configuration;

	@Test
	public void heartbeat() {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		Node node1 = new ServerNode();
		node1.setId(1);
		node1.setUrl("http://server/1");
		nodeRouter.heartbeat(node1);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1));

		// add new node
		Node node2 = new ServerNode();
		node2.setId(2);
		node2.setUrl("http://server/2");
		nodeRouter.heartbeat(node2);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1, node2));
		assertEquals(nodeRouter.getNodesCount(), 2);
		assertEquals(nodeRouter.getNodeByKey(1l), node2);
		assertEquals(nodeRouter.getNodeByKey(2l), node1);
	}

	@Test
	public void heartbeatFromTheSameNode() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		Node node = null;

		for (int i = 0; i < 10; i++) {
			node = configuration.getSelfNode();
			nodeRouter.heartbeat(node);
		}

		assertEquals(nodeRepository.getNodes().size(), 1);
		assertEquals(nodeRepository.getNodes(), ImmutableMap.of(0, node));
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node));
		assertEquals(nodeRouter.getNodeByKey(1l), node);
		assertEquals(nodeRouter.getNodeByKey(2l), node);
		assertEquals(nodeRouter.getNodeByKey(3l), node);
		assertEquals(nodeRouter.getNodeByKey(4l), node);
	}

	@Test
	public void autoRemoveNode() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		Node node1 = new ServerNode();
		node1.setId(1);
		node1.setUrl("http://server/1");
		nodeRouter.heartbeat(node1);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1));

		// assume, heartbeat from another server
		Node node2 = new ServerNode();
		node2.setId(2);
		node2.setUrl("http://server/2");
		nodeRouter.heartbeat(node2);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1, node2));

		Thread.sleep((configuration.getHeartbeatDelay() + 3) * 1000);
		// assume, heartbeat from another server
		nodeRouter.heartbeat(node2);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node2));
	}

	@Test
	public void expirationLogic() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		Node node1 = new ServerNode();
		node1.setId(1);
		node1.setUrl("http://server/1");
		nodeRouter.heartbeat(node1);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1));

		// assume, heartbeat from another server
		Node node2 = new ServerNode();
		node2.setId(2);
		node2.setUrl("http://server/2");
		nodeRouter.heartbeat(node2);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1, node2));

		// expire time is 2000, but we have additional 2000 gap wrt inaccurate repository backend
		Thread.sleep((configuration.getHeartbeatDelay() + 1) * 1000);
		// assume, heartbeat from another server
		nodeRouter.heartbeat(node2);

		assertEquals(nodeRouter.getNodes(), ImmutableSet.of(node1, node2));
	}

	@Test
	public void benchmark() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		long start = currentTimeMillis();

		for (int i = 0; i < 32; i++) {
			Node node = new ServerNode();
			node.setId(i + 1);
			node.setUrl("http://server/" + (i + 1));

			nodeRouter.heartbeat(node);
		}

		log.info("32 servers heartbeat: {}", (currentTimeMillis() - start));

		Thread.sleep(50l);

		assertEquals(nodeRouter.getNodes().size(), 32);
	}

	@Test(timeOut = 5000)
	public void multiProcessHeartbeat() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		ExecutorService executorService = Executors.newFixedThreadPool(64);

		final Set<Node> nodes = newHashSet();

		for (int i = 0; i < 32; i++) {
			Node node = new ServerNode();
			node.setId(i);
			node.setUrl("http://server/" + i);
			nodes.add(node);
		}

		for (final Node node : nodes) {
			executorService.submit(
				new Runnable() {
					@Override
					public void run() {
						try {
							nodeRouter.heartbeat(node);
						} catch (Exception e) {
							log.error("Can't heartbeat", e);
						}
					}
				}
			);
		}

		while (nodeRouter.getNodes().size() != nodes.size()) {
			log.info("Current size is: {}", nodeRouter.getNodes().size());
			Thread.sleep(50);
		}
	}

	@Test(timeOut = 5000)
	public void multiProcessListenerTest() throws InterruptedException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		UniqueCounterListener countableListener = spy(new UniqueCounterListener());
		nodeRepository.setListener(countableListener);

		ExecutorService executorService = Executors.newFixedThreadPool(64);

		final Set<Node> nodes = newHashSet();

		for (int i = 0; i < 32; i++) {
			Node node = new ServerNode();
			node.setId(i);
			node.setUrl("http://server/" + i);
			nodes.add(node);
		}

		for (final Node node : nodes) {
			executorService.submit(
				new Runnable() {
					@Override
					public void run() {
						try {
							nodeRouter.heartbeat(node);
						} catch (Exception e) {
							log.error("Can't heartbeat", e);
						}
					}
				}
			);
		}

		while (nodeRouter.getNodes().size() != nodes.size()) {
			Thread.sleep(50);
		}

		assertEquals(nodeRouter.getNodes(), nodes);
		verify(countableListener, times(nodes.size())).onChange(
			Matchers.<Map<Integer, Node>>any(),
			Matchers.<Map<Integer, Node>>any()
		);

		assertEquals(countableListener.getDifference().size(), nodes.size());
	}

	@Test(timeOut = 5000)
	public void multiProcessAddNode() throws InterruptedException, ExecutionException {
		assertEquals(nodeRouter.getNodes(), ImmutableSet.of());

		ExecutorService executorService = Executors.newFixedThreadPool(64);

		final Set<Node> nodes = newHashSet();

		for (int i = 0; i < 32; i++) {
			Node node = new ServerNode();
			node.setId(i);
			node.setUrl("http://server/" + i);
			nodes.add(node);
		}

		for (final Node node : nodes) {
			executorService.submit(
				new Runnable() {
					@Override
					public void run() {
						try {
							nodeRepository.add(node);
						} catch (Exception e) {
							log.error("Can't add noe", e);
						}
					}
				}
			);
		}

		while (nodeRouter.getNodes().size() != nodes.size()) {
			Thread.sleep(50);
		}
	}

	@Test
	public void onChange() throws InterruptedException {
		assertEquals(nodeRepository.getNodes(), ImmutableMap.of());

		Listener equalsListener = spy(new DefaultListener());

		Node newNode = new ServerNode();
		newNode.setId(1);
		newNode.setUrl("http://server/1");

		nodeRepository.setListener(equalsListener);
		nodeRouter.heartbeat(newNode);

		// new node has been added
		verify(equalsListener).onChange(
			eq(ImmutableMap.<Integer, Node>of()),
			eq(ImmutableMap.<Integer, Node>of(0, newNode))
		);

		nodeRouter.heartbeat(newNode);

		// nothing changed
		verify(equalsListener).onChange(
			eq(ImmutableMap.<Integer, Node>of(0, newNode)),
			eq(ImmutableMap.<Integer, Node>of(0, newNode))
		);

		Thread.sleep(4100l);

		Node anotherNode = new ServerNode();
		anotherNode.setId(2);
		anotherNode.setUrl("http://server/2");

		nodeRouter.heartbeat(anotherNode);

		// node one is auto-removed and node two is added
		verify(equalsListener).onChange(
			eq(ImmutableMap.<Integer, Node>of(0, newNode)),
			eq(ImmutableMap.<Integer, Node>of(0, anotherNode))
		);
	}

	@BeforeMethod
	public void cleanUp() throws InterruptedException {
		nodeRepository.removeAll();
	}
}
