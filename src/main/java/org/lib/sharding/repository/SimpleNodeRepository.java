package org.lib.sharding.repository;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Singleton
public class SimpleNodeRepository implements NodeRepository {
    private static final Logger log = LoggerFactory.getLogger(SimpleNodeRepository.class);

    private final List<ClusterNode> nodes = new ArrayList<>();

    @NotNull
    @Override
    public List<ClusterNode> getNodes() {
        synchronized (nodes) {
            return ImmutableList.copyOf(nodes);
        }
    }

    @Override
    public void sync(@NotNull Set<ClusterNode> actual, @NotNull ClusterNode self) {
        synchronized (nodes) {
            Set<ClusterNode> uniqueNodes = ImmutableSet.copyOf(nodes);

            Sets.SetView<ClusterNode> removedNodes = Sets.difference(uniqueNodes, actual);
            Sets.SetView<ClusterNode> addedNodes = Sets.difference(actual, uniqueNodes);

            for (ClusterNode added : addedNodes) {
                log.info("Adding node to the list with [{}] on node [{}]", added, self);
                add(added);
            }

            for (ClusterNode removed : removedNodes) {
                log.info("Removing node from the list [{}] on node [{}]", removed, self);
                remove(removed);
            }
        }
    }

    @Override
    public void add(@NotNull ClusterNode node) {
        synchronized (nodes) {
            nodes.add(node);
        }
    }

    @Override
    public void remove(@NotNull ClusterNode node) {
        synchronized (nodes) {
            int removedIndex = nodes.indexOf(node);
            nodes.set(removedIndex, nodes.get(nodes.size() - 1));
            nodes.remove(nodes.get(nodes.size() - 1));
        }
    }

    @Override
    public int size() {
        synchronized (nodes) {
            return nodes.size();
        }
    }

    @Override
    public void setNodes(@NotNull List<ClusterNode> nodes) {
        synchronized (this.nodes) {
            this.nodes.clear();
            this.nodes.addAll(nodes);
        }
    }
}
