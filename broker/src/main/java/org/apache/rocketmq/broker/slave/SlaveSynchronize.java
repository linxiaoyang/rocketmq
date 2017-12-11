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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从master上拉去数据进行同步到本地
 */
public class SlaveSynchronize {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    /**
     * 在集群模式下，为了保证高可用，必须要保证备用Broker与主用Broker信息是一致的，
     * 在备用Broker初始化时设置的了定时任务，每个60秒调用SlaveSynchronize.syncAll()
     * 方法发起向主用Broker进行一次config类文件的同步，而消息数据的同步由主备Broker通过
     * 心跳检测的方式完成，每隔5秒进行一次心跳。 主用Broker提供读写服务，而备用Broker只
     * 提供读服务。
     */
    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
    }

    /**
     * Topic配置同步
     * <p>
     * 在syncAll方法中调用SlaveSynchronize.syncTopicConfig()方法向主用Broker发起topics.json文件的同步。大致步骤如下：
     * <p>
     * 1）向主用Broker发起GET_ALL_TOPIC_CONFIG请求码，主用Broker将所有topic配置信息返回给备用Broker；
     * <p>
     * 2）比较主备topic信息的DataVersion；若不同则用主用Broker返回的topic配置信息更新备用Broker的topic，并进行持久化，同时更新备用Broker中topic信息的DataVersion。
     */
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                TopicConfigSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                        .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                            .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                            .putAll(topicWrapper.getTopicConfigTable());
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 消费进度信息同步
     * <p>
     * 在syncAll方法中调用SlaveSynchronize. syncConsumerOffset ()方法向主用Broker发起consumerOffset.json文件的同步。大致步骤如下：
     * <p>
     * 1）向主用Broker发起GET_ALL_CONSUMER_OFFSET请求码，主用Broker将所有ConsumerOffset配置信息返回给备用Broker；
     * <p>
     * 2）更新备用Broker的ConsumerOffsetManager.offsetTable变量，同时进行持久化；
     */
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                        .putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 延迟消费进度信息同步
     * <p>
     * 在syncAll方法中调用SlaveSynchronize.syncDelayOffset()方法向主用Broker发起delayOffset.json文件的同步。大致步骤如下：
     * <p>
     * 向主用Broker发起GET_ALL_DELAY_OFFSET请求码，主用Broker将所有delayOffset信息返回给备用Broker；备用Broker直接将收到的delayOffset信息持久化到物理文件delayOffset.json中；
     */
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 订阅关系同步
     * <p>
     * 在syncAll方法中调用SlaveSynchronize. syncSubscriptionGroupConfig ()方法向主用Broker发起delayOffset.json文件的同步。大致步骤如下：
     * <p>
     * 1）向主用Broker发起GET_ALL_SUBSCRIPTIONGROUP_CONFIG请求码，主用Broker将所有SubscriptionGroup配置信息返回给备用Broker；
     * <p>
     * 2）更新备用Broker的ConsumerOffsetManager. subscriptionGroupTable变量，同时进行持久化；
     */
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                            this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                            subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                            subscriptionWrapper.getSubscriptionGroupTable());
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
