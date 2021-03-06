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
import com.google.inject.Injector;
import com.google.inject.Module;
import org.lib.sharding.configuration.FirstServerRootModule;
import org.lib.sharding.configuration.SecondServerRootModule;
import org.testng.annotations.BeforeMethod;

import java.util.Set;

import static com.google.inject.Guice.createInjector;

public abstract class BaseMultipleInjectorTest {
    protected Injector firstServerInjector;
    protected Injector secondServerInjector;
    protected Injector secondRestartedServerInjector;

    @BeforeMethod
    protected void createInjectors() throws Exception {
        firstServerInjector = createInjector(getFirstInjectorModules());
        secondServerInjector = createInjector(getSecondInjectorModules());
    }

    protected Set<Module> getFirstInjectorModules() {
        return ImmutableSet.of(new FirstServerRootModule());
    }

    protected Set<Module> getSecondInjectorModules() {
        return ImmutableSet.of(new SecondServerRootModule());
    }
}
