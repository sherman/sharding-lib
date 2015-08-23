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

import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;
import org.lib.sharding.domain.ClusterNode;
import org.lib.sharding.repository.NodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static com.google.common.hash.Hashing.consistentHash;
import static com.google.common.hash.Hashing.md5;

abstract class BaseNodeRouter<T> implements NodeRouter<T> {
    private static final Logger log = LoggerFactory.getLogger(BaseNodeRouter.class);

    abstract protected NodeRepository getNodeRepository();

    @Override
    public ClusterNode getNodeByKey(@NotNull T elt) {
        List<ClusterNode> nodes = getNodeRepository().getNodes();
        if (nodes.isEmpty()) {
            throw new IllegalStateException("Nodes list is empty!");
        }

        int bucket = consistentHash(md5().hashString(elt.toString(), Charset.forName("UTF-8")), nodes.size());

        ClusterNode node = nodes.get(bucket);
        if (null == node) {
            throw new IllegalStateException("Can't find node for the given key:" + elt.toString());
        }

        return node;
    }

    @Override
    public int getNodesCount() {
        return getNodeRepository().size();
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return ImmutableSet.copyOf(getNodeRepository().getNodes());
    }
}
