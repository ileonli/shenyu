/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.discovery.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.shenyu.common.exception.ShenyuException;
import org.apache.shenyu.discovery.api.ShenyuDiscoveryService;
import org.apache.shenyu.discovery.api.config.DiscoveryConfig;
import org.apache.shenyu.discovery.api.listener.DataChangedEventListener;
import org.apache.shenyu.discovery.api.listener.DiscoveryDataChangedEvent;
import org.apache.shenyu.spi.Join;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * The type Zookeeper for shenyu discovery service.
 */
@Join
public class ZookeeperDiscoveryService implements ShenyuDiscoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperDiscoveryService.class);

    private CuratorFramework client;

    private final Map<String, String> nodeDataMap = new HashMap<>();

    private final Map<String, TreeCache> cacheMap = new HashMap<>();

    @Override
    public void init(final DiscoveryConfig config) {
        String baseSleepTimeMilliseconds = config.getProps().getProperty("baseSleepTimeMilliseconds", "1000");
        String maxRetries = config.getProps().getProperty("maxRetries", "3");
        String maxSleepTimeMilliseconds = config.getProps().getProperty("maxSleepTimeMilliseconds", "1000");
        String connectionTimeoutMilliseconds = config.getProps().getProperty("connectionTimeoutMilliseconds", "1000");
        String sessionTimeoutMilliseconds = config.getProps().getProperty("sessionTimeoutMilliseconds", "1000");
        String namespace = config.getProps().getProperty("namespace", "");
        String digest = config.getProps().getProperty("digest", null);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(Integer.parseInt(baseSleepTimeMilliseconds), Integer.parseInt(maxRetries), Integer.parseInt(maxSleepTimeMilliseconds));
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(config.getServerList())
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(Integer.parseInt(connectionTimeoutMilliseconds))
                .sessionTimeoutMs(Integer.parseInt(sessionTimeoutMilliseconds))
                .namespace(namespace);
        if (StringUtils.isNoneBlank(digest)) {
            builder.authorization("digest", digest.getBytes(StandardCharsets.UTF_8));
        }
        this.client = builder.build();
        this.start();
    }

    private void start() {
        this.client.getConnectionStateListenable().addListener((c, newState) -> {
            if (newState == ConnectionState.RECONNECTED) {
                nodeDataMap.forEach((k, v) -> {
                    if (!this.exits(k)) {
                        this.createOrUpdate(k, v, CreateMode.EPHEMERAL);
                        LOGGER.info("Zookeeper client register instance success: key={}|value={}", k, v);
                    }
                });
            }
        });
        this.client.start();
        try {
            if (!this.client.blockUntilConnected(30, TimeUnit.SECONDS)) {
                throw new ShenyuException("Shenyu start ZookeeperDiscoveryService failure 30 seconds timeout");
            }
        } catch (InterruptedException e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public boolean exits(final String key) {
        try {
            return null != client.checkExists().forPath(key);
        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }

    private void createOrUpdate(final String key, final String value, final CreateMode mode) {
        String val = StringUtils.isEmpty(value) ? "" : value;
        try {
            this.client.create().orSetData().creatingParentsIfNeeded().withMode(mode).forPath(key, val.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public void watch(final String key, final DataChangedEventListener listener) {
        try {
            TreeCache treeCache = new TreeCache(client, key);
            TreeCacheListener treeCacheListener = (curatorFramework, event) -> {
                final ChildData data = event.getData();
                if (Objects.isNull(data) || Objects.isNull(data.getData())) {
                    return;
                }

                final String currentPath = data.getPath();
                final String currentData = new String(data.getData(), StandardCharsets.UTF_8);
                LOGGER.info("Shenyu find resultData = {}", currentData);

                final Stat stat = data.getStat();
                boolean isEphemeral = Objects.nonNull(stat) && stat.getEphemeralOwner() > 0;
                if (!isEphemeral) {
                    LOGGER.info("Shenyu Ignore non-ephemeral node changes");
                    return;
                }

                DiscoveryDataChangedEvent.Event eventType;
                switch (event.getType()) {
                    case NODE_ADDED:
                        eventType = DiscoveryDataChangedEvent.Event.ADDED;
                        break;
                    case NODE_UPDATED:
                        eventType = DiscoveryDataChangedEvent.Event.UPDATED;
                        break;
                    case NODE_REMOVED:
                        eventType = DiscoveryDataChangedEvent.Event.DELETED;
                        break;
                    default:
                        eventType = DiscoveryDataChangedEvent.Event.IGNORED;
                        break;
                }
                listener.onChange(new DiscoveryDataChangedEvent(currentPath, currentData, eventType));
            };
            treeCache.getListenable().addListener(treeCacheListener);
            treeCache.start();
            cacheMap.put(key, treeCache);
        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public void unWatch(final String key) {
        TreeCache removed = cacheMap.remove(key);
        if (Objects.nonNull(removed)) {
            removed.close();
        }
    }

    @Override
    public void register(final String key, final String value) {
        this.createOrUpdate(key, value, CreateMode.PERSISTENT);
    }

    @Override
    public List<String> getRegisterData(final String key) {
        try {
            List<String> children = client.getChildren().forPath(key);
            List<String> registerData = new ArrayList<>(children.size());
            for (String child : children) {
                String nodePath = key + "/" + child;
                byte[] data = client.getData().forPath(nodePath);
                registerData.add(new String(data, StandardCharsets.UTF_8));
            }
            return registerData;
        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public void close() {
        try {
            // close treeCache
            for (TreeCache value : cacheMap.values()) {
                value.close();
            }
            client.close();
        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }
}
