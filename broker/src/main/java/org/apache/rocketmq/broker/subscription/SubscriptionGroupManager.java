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
package org.apache.rocketmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionGroupManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }
    }

    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }
    }

    /**
     * 根据GroupName查找订阅组信息
     * <p>
     * <p>
     * 调用SubscriptionGroupManager.findSubscriptionGroupConfig(String GroupNmae)方法获取订阅信息。大致步骤如下：
     * 1、以GroupNmae从SubscriptionGroupManager.SubscriptionGroupTable变量中获取订阅信息SubscriptionGroupConfig对象；若不为空则直接返回该对象；
     * 2、若为空，表示还没有该订阅信息，再检查Broker是否运行自动创建订阅组（由配置参数autoCreateSubscriptionGroup设置，默认为true）。
     * 若不允许则直接返回null；若允许则首先以GroupName为参数创建SubscriptionGroupConfig对象，并存入SubscriptionGroupTable变量中；
     * 然后更新SubscriptionGroupManager的dataversion值；再将SubscriptionGroupManager的内容持久化到subscriptionGroup.json文件中；
     * 最后返回该新创建的SubscriptionGroupConfig对象；
     *
     * @param group
     * @return
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                SubscriptionGroupConfig preConfig = this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                if (null == preConfig) {
                    log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                }
                this.dataVersion.nextVersion();
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }
}
