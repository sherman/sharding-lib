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
import com.google.inject.Module;
import org.lib.sharding.configuration.FirstServerRootModule;
import org.lib.sharding.configuration.SecondServerRootModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;

public class ClusterMembershipServiceTest extends BaseMultipleInjectorTest {
    private static final Logger log = LoggerFactory.getLogger(ClusterMembershipServiceTest.class);

    private ClusterMembershipService membershipService1;

    private ClusterMembershipService membershipService2;

    @Test
    public void startEmpty() {
        membershipService1.start("node1");

        assertEquals(membershipService1.getSelfNode().getAddress().toString(), "node1");

        membershipService1.stop();
    }

    @Test
    public void startTwoNodes() throws InterruptedException {
        membershipService1.start("node1");
        membershipService2.start("node2");

        assertEquals(membershipService1.getNodes().size(), 2);
        assertEquals(membershipService2.getNodes().size(), 2);
        assertEquals(membershipService1.getNodes(), membershipService2.getNodes());

        membershipService1.stop();
        membershipService2.stop();
    }

    @Test
    public void removeNodesFromTheEnd() throws InterruptedException {
        membershipService1.start("node1");
        membershipService2.start("node2");

        assertEquals(membershipService1.getNodes().size(), 2);
        assertEquals(membershipService2.getNodes().size(), 2);
        assertEquals(membershipService1.getNodes(), membershipService2.getNodes());

        membershipService2.stop();
        Thread.sleep(10);
        assertEquals(membershipService1.getNodes().size(), 1);

        membershipService2.start("node2");

        assertEquals(membershipService1.getNodes(), membershipService2.getNodes());
        assertEquals(membershipService1.getNodes().get(0).getAddress().toString(), "node1");

        membershipService1.stop();
        membershipService2.stop();
    }

    @BeforeMethod(dependsOnMethods = "createInjectors")
    protected void setUp() {
        membershipService1 = firstServerInjector.getInstance(ClusterMembershipService.class);

        membershipService2 = secondServerInjector.getInstance(ClusterMembershipService.class);
    }

    @Override
    protected Set<Module> getFirstInjectorModules() {
        return ImmutableSet.of(
                new FirstServerRootModule()
        );
    }

    @Override
    protected Set<Module> getSecondInjectorModules() {
        return ImmutableSet.of(
                new SecondServerRootModule()
        );
    }
}
