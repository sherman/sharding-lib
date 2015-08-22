package org.lib.sharding.service;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.Util;
import org.lib.sharding.configuration.ClusterPropertiesConfiguration;
import org.lib.sharding.domain.ClusterNode;
import org.lib.sharding.repository.NodeRepository;
import org.lib.sharding.util.GuavaCollectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.jgroups.jmx.JmxConfigurator.registerChannel;
import static org.jgroups.util.Util.createConcurrentMap;
import static org.jgroups.util.Util.getMBeanServer;

@Singleton
public class ClusterMembershipServiceImpl extends ReceiverAdapter implements ClusterMembershipService {
    private static final Logger log = LoggerFactory.getLogger(ClusterMembershipServiceImpl.class);

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String CLUSTER_LOCK = "cluster_lock";

    private static final Short GET_NODE = 0x1;
    private static final Short SET_NODES = 0x2;

    private RpcDispatcher dispatcher = null;
    private JChannel channel;
    private volatile ClusterNode self;

    private final ExecutorService updateNodesExecutor = Executors.newFixedThreadPool(
            4,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("NodesUpdater-%d")
                    .build()
    );

    private static final Map<Short, Method> methods = createConcurrentMap(1);

    static {
        try {
            methods.put(GET_NODE, ClusterMembershipServiceImpl.class.getMethod("getSelfNode"));
            methods.put(SET_NODES, ClusterMembershipServiceImpl.class.getMethod("setNodes", List.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Inject
    private ClusterPropertiesConfiguration configuration;

    @Inject
    private LockService lockService;

    @Inject
    private NodeRepository nodeRepository;

    @Inject
    private RebalancingStrategy rebalancingStrategy;

    @Override
    public void start(@NotNull String name) {
        try {
            channel = new JChannel(ClusterMembershipServiceImpl.class.getClassLoader().getResourceAsStream(configuration.getClusterConfig()));
            channel.setDiscardOwnMessages(true);
            channel.setName(name);

            dispatcher = new RpcDispatcher(channel, null, this, this);
            dispatcher.setMethodLookup(methods::get);

            dispatcher.setMessageListener(this);

            channel.connect(CLUSTER_NAME);

            channel.getState(null, 10000);

            log.info("Node is started [{}]", getSelfNode());

            registerChannel(channel, getMBeanServer(), name);
        } catch (Exception e) {
            log.error("Can't create channel", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        channel.close();

        log.debug("Node [{}] is stopped", self);

        self = null;
    }

    @Override
    public JChannel getChannel() {
        return channel;
    }

    @Override
    public ClusterNode getSelfNode() {
        if (self == null) {
            self = new ClusterNode(channel.getAddress());
        }
        return self;
    }

    @Override
    public ClusterNode getNode(@NotNull Address address) {
        log.debug("Getting node info about node [{}] on node [{}]", address, self);

        if (getSelfNode().getAddress().equals(address)) {
            return getSelfNode();
        }

        try {
            return dispatcher.callRemoteMethod(
                    address,
                    new MethodCall(GET_NODE),
                    new RequestOptions(ResponseMode.GET_FIRST, 1000L));
        } catch (Exception e) {
            log.error("Can't call method " + GET_NODE + " on remote node " + address, e);
        }

        return null;
    }

    @Override
    public List<ClusterNode> getNodes() {
        return nodeRepository.getNodes();
    }

    @Override
    public void setNodes(@NotNull Address address, @NotNull List<ClusterNode> nodes) {
        log.debug("Set new nodes list for node [{}] on node [{}]", address, getSelfNode());

        if (getSelfNode().getAddress().equals(address)) {
            setNodes(nodes);
            return;
        }

        try {
            dispatcher.callRemoteMethod(
                    address,
                    new MethodCall(SET_NODES, nodes),
                    new RequestOptions(ResponseMode.GET_FIRST, 1000L));
        } catch (Exception e) {
            log.error("Can't call method " + SET_NODES + " on remote node " + address, e);
        }
    }

    public void setNodes(List<ClusterNode> nodes) {
        nodeRepository.setNodes(nodes);
    }


    @Override
    public void receive(Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Message received:" + message);
        }
    }

    @Override
    public void viewAccepted(final View view) {
        super.viewAccepted(view);

        log.info("Nodes list is changed {} for node [{}], creator [{}]", view, getSelfNode(), view.getCreator());

        updateNodesExecutor.submit(
                () -> {
                    try {
                        update();
                    } catch (Exception e) {
                        log.error("Can't update nodes list", e);
                    }
                }
        );

    }

    @Override
    public void getState(OutputStream output) throws Exception {
        Util.objectToStream(nodeRepository.getNodes(), new DataOutputStream(output));
    }

    @Override
    public void setState(InputStream input) throws Exception {
        List<ClusterNode> state = (List<ClusterNode>) Util.objectFromStream(new DataInputStream(input));
        nodeRepository.setNodes(state);
    }

    private synchronized void update() {
        lockService.setChannel(channel);

        boolean acquired = false;
        Lock lock = lockService.getLock(CLUSTER_LOCK);
        try {
            acquired = lock.tryLock(5000L, TimeUnit.MILLISECONDS);
            if (!acquired) {
                return;
            }

            log.trace("Locked");

            View view = channel.getView();

            List<ClusterNode> oldNodes = nodeRepository.getNodes();

            // update nodes
            log.debug("Current nodes list are [{}] on node [{}]", oldNodes, getSelfNode());

            Set<ClusterNode> actualNodes = view.getMembers()
                    .stream()
                    .map(this::getNode)
                    .collect(GuavaCollectors.toImmutableSet());

            nodeRepository.sync(actualNodes, getSelfNode());

            List<ClusterNode> newNodes = nodeRepository.getNodes();

            log.info("New node list is [{}] on node [{}]", newNodes, getSelfNode());

            // update cluster
            final List<ClusterNode> finalCurrentNodes = newNodes;

            view.getMembers()
                    .stream()
                    .filter(address -> !getSelfNode().getAddress().equals(address))
                    .forEach(address -> setNodes(address, finalCurrentNodes));

            rebalancingStrategy.nodesChanged(oldNodes, newNodes);
        } catch (Exception e) {
            log.error("Can't update nodes list", e);
        } finally {
            if (acquired) {
                lock.unlock();
                log.trace("Unlocked");
            }
        }
    }
}
