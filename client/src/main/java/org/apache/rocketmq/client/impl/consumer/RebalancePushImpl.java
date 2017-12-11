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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
            && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        return this.unlockDelay(mq, pq);
                    } finally {
                        pq.getLockConsume().unlock();
                    }
                } else {
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                        mq,
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }


    /**
     *
     *
     * 对于PUSH模式（RebalancePushImpl），computePullFromWhere方法的计算逻辑如下：

     1、获取DefaultMQPushConsumer.consumeFromWhere变量值，在应用层设置该值，默认值为CONSUME_FROM_LAST_OFFSET；

     2、获取DefaultMQPushConsumerImpl.offsetStore对象，若为广播模式则该对象初始化为LocalFileOffsetStore；若为集群模式则该对象初始化为RemoteBrokerOffsetStore对象；

     3、若consumeFromWhere值等于CONSUME_FROM_LAST_OFFSET（表示一个新的订阅组第一次启动从队列的最后位置开始消费）。

     3.1）调用offsetStore.readOffset(MessageQueue mq, ReadOffsetType type)方法，其中type=ReadOffsetType.READ_FROM_STORE。

     3.1.1)对于LocalFileOffsetStore对象，从本地加载offsets.json文件，然后获取该MessageQueue对象的offset值；

     3.1.2)对于RemoteBrokerOffsetStore对象,获取逻辑如下：

     A）以MessageQueue对象的brokername从MQClientInstance.brokerAddrTable中获取Broker的地址；若没有获取到则立即调用updateTopicRouteInfoFromNameServer方法然后再次获取；

     B）构造QueryConsumerOffsetRequestHeader对象，其中包括topic、consumerGroup、queueId；然后调用MQClientAPIImpl.queryConsumerOffset (String addr, QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis)方法向Broker发送QUERY_CONSUMER_OFFSET请求码，获取消费进度Offset；

     C）用上一步从Broker获取的offset更新本地内存的消费进度列表数据RemoteBrokerOffsetStore.offsetTable:ConcurrentHashMap<MessageQueue, AtomicLong>变量值；

     D）返回该offset值；

     3.2)检查上一步返回的offset值：

     3.2.1)若该offset值大于零，则直接返回该值；

     3.2.2)若该offset值等于-1：若topic是以"%RETRY%"开头，则返回0；若不是以此字符开头，则向该MessageQueue对象的brokername所在Broker发送GET_MAX_OFFSET请求码，获取该MessageQueue对象的topic和queueId对应的逻辑队列的最大逻辑偏移量，并返回该最大offset值；

     3.2.3)否则直接返回-1；

     4、若consumeFromWhere值等于CONSUME_FROM_FIRST_OFFSET（表示一个新的订阅组第一次启动从队列的最前位置开始消费）。

     4.1）调用offsetStore.readOffset(MessageQueue mq, ReadOffsetType type)方法，其中type=ReadOffsetType.READ_FROM_STORE。逻辑与上面的3.1一样。

     4.2)检查上一步返回的offset值：

     若大于零，则返回该值；若等于-1，则返回0；否则返回-1；

     5、若consumeFromWhere值等于CONSUME_FROM_TIMESTAMP（表示一个新的订阅组第一次启动从指定时间点开始消费）。

     5.1）调用offsetStore.readOffset(MessageQueue mq, ReadOffsetType type)方法，其中type=ReadOffsetType.READ_FROM_STORE。逻辑与上面的3.1一样。

     5.2）若上一步返回的offset值大于0，则返回该offset值；

     5.3)若返回的offset值等于-1；若topic是以"%RETRY%"开头，则向该MessageQueue对象的brokername所在Broker发送GET_MAX_OFFSET请求码，获取该MessageQueue对象的topic和queueId对应的逻辑队列的最大逻辑偏移量，并返回该最大offset值；否则取DefaultMQPushConsumer.consumeTimestamp变量的时间戳，向该MessageQueue对象的brokername所在Broker发送SEARCH_OFFSET_BY_TIMESTAMP请求码，获取距离该时间戳最近的逻辑队列的offset值，并返回该offset值；

     5.4）否则直接返回-1；
     *
     *
     * @param mq
     * @return
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }

        return result;
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }
}
