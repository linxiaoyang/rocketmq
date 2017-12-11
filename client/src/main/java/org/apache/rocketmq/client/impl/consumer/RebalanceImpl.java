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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.slf4j.Logger;

/**
 * Base class for rebalance algorithm
 */
public abstract class RebalanceImpl {
    protected static final Logger log = ClientLogger.getLog();
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
            new ConcurrentHashMap<String, Set<MessageQueue>>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
            new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
                         AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                         MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                        this.consumerGroup,
                        this.mQClientFactory.getClientId(),
                        mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                        lockOK ? "OK" : "Failed",
                        this.consumerGroup,
                        mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                            this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public void doRebalance(final boolean isOrder) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }


    /**
     * @param topic
     * @param isOrder
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            /**
             *      * 检查RebalanceImpl.messageModel变量，若等于BROADCASTING，则执行RebalanceImpl. rebalanceByTopic (String topic)方法中的广播模式下的逻辑，该方法的主要目的是为topic下的所有MessageQueue集合中的每个MessageQueue对象创建ProcessQueue对象，并且对于PUSH模式将拉取消息的请求放入服务线程（PullMessageService）中。大致逻辑如下：
             * <p>
             * 1、以topic值为key值从RebalanceImpl.topicSubscribeInfoTable: ConcurrentHashMap<String/* topic , Set<MessageQueue>>列表中获取MessageQueue集合；
             * <p>
             * 2、以topic和上一步获得的MessageQueue集合为参数调用RebalanceImpl.updateProcessQueueTableInRebalance(String topic, Set<MessageQueue> mqSet)方法并获取返回结果。
             * <p>
             * 3、若返回结果为true（即RebalanceImpl.processQueueTable列表有变化）则调用RebalanceImpl.messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided)方法；对于PUSH模式下面没有相应的处理逻辑，即messageQueueChanged方法为空；对于PULL模式下会调用RebalancePullImpl对象的messageQueueChanged方法，此方法的目的是对于采用了计划消息拉取服务的应用来说，当Consumer订阅的MessageQueue有变动或者或者ProcessQueue有更新时触发消息拉取动作，大致逻辑如下：
             * <p>
             * 3.1）获取DefaultMQPullConsumer.messageQueueListener变量的值，该变量只有在应用层使用MQPullConsumerScheduleService类来进行计划拉取消息服务时才会设置MessageQueueListener对象；该对象是MQPullConsumerScheduleService类的内部类MessageQueueListenerImpl类的实例；
             * <p>
             * 3.2）调用该内部类的实例MessageQueueListener对象的messageQueueChanged方法；
             */
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                                consumerGroup,
                                topic,
                                mqSet,
                                mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }

            /**
             * 检查RebalanceImpl.messageModel变量，若等于CLUSTERING，则执行RebalanceImpl. rebalanceByTopic (String topic)方法中的集群模式下的逻辑，该方法的主要目的是首先为topic分配MessageQueue集合，然后为集合中的每个MessageQueue对象创建ProcessQueue对象，最后对于PUSH模式将拉取消息的请求放入服务线程（PullMessageService）中。

             1、以入参topic值从RebalanceImpl.topicSubscribeInfoTable: ConcurrentHashMap<String/* topic , Set<MessageQueue>>变量中获取对应的MessageQueue集合（命名为mqSet）；

             2、获取topic的Broker地址。以入参topic值从MQClientInstance.topicRouteTable:ConcurrentHashMap<String/* Topic,TopicRouteData>变量中获取对应的TopicRouteData对象，然后取该TopicRouteData对象的brokerDatas:List<BrokerData>集合；若该集合为空则直接返回null；否则若该集合不为空，则取第一个BrokerData对象，从该对象的brokerAddrs: HashMap<Long/* brokerId , String/* broker address >变量中获取brokerId=0的Broker地址（即主用Broker），若没有主用Broker则获取备用Broker地址；

             3、若上一步获取的Broker地址为空，则调用MQClientInstance. updateTopicRouteInfoFromNameServer方法从NameServer获取之后再按第2步的方法查找topic对应的Broker地址；循环执行直到找到Broker地址为止；

             4、以consumerGroup为参数向该Broker发送GET_CONSUMER_LIST_BY_GROUP请求码，获取以该consumerGroup为名的ClientId集合；在Broker收到请求之后，从ConsumerManager.consumerTable中获取ConsumerGroupInfo对象，然后遍历该对象的channelInfoTable集合，将集合的每个ClientChannelInfo对象的clientId变量值作为ClientId集合；

             5、若第1步的MessageQueue集合（名为mqSet）和上一步的ClientId集合都不为null则继续进行下面操作，否则直接退出该方法；

             6、则调用RebalanceImpl. allocateMessageQueueStrategy.allocate (String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll)为当前Consumer分配应该消费的MessageQueue集合队列（命名allocateResultSet）；有四种分配策略，包括平均分配（AVG）、AVG_BY_CIRCLE 、CONFIG 、MACHINE_ROOM，默认为AllocateMessageQueueAveragely分配策略；

             6、以topic和上一步分配的MessageQueue集合（名为allocateResultSet）为参数调用RebalanceImpl. updateProcessQueueTableInRebalance (String topic, Set<MessageQueue> mqSet)方法，该方法是对每个MessageQueue创建一个ProcessQueue对象并存入RebalanceImpl.processQueueTable队列中，然后对于PUSH模式下向后台线程放入拉取消息的请求对象，最后一个标记位表示是否对RebalanceImpl.processQueueTable列表有修改，若有则返回true；（详见5.3.3小节）

             7、若上一步的返回结果为true（即RebalanceImpl.processQueueTable列表有记录变化时），则调用RebalanceImpl.messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided)方法，其中mqAll为第1步的MessageQueue集合（名为mqSet），mqDivided为第6步的MessageQueue集合（名为allocateResultSet）。对于PUSH模式下面没有相应的处理逻辑，即messageQueueChanged方法为空；对于PULL模式下会调用RebalancePullImpl对象的messageQueueChanged方法，此方法的目的是对于采用了计划消息拉取服务的应用来说，当Consumer订阅的MessageQueue有变动或者或者ProcessQueue有更新时触发消息拉取动作，大致逻辑如下：

             7.1）获取DefaultMQPullConsumer.messageQueueListener变量的值，该变量只有在应用层使用MQPullConsumerScheduleService类来进行计划拉取消息服务时才会设置MessageQueueListener对象；该对象是MQPullConsumerScheduleService类的内部类MessageQueueListenerImpl类的实例；

             7.2）调用该内部类的实例MessageQueueListener对象的messageQueueChanged方法；
             */
            case CLUSTERING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        allocateResult = strategy.allocate(
                                this.consumerGroup,
                                this.mQClientFactory.getClientId(),
                                mqAll,
                                cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                                e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                                "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                                allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }


    /**
     * 为每个消息队列维护一个消息处理队列
     * <p>
     * <p>
     * <p>
     * <p>
     * 一个MessageQueue消息队列的消息分配一个消息处理队列ProcessQueue来进行消费处理，这两个队列的关系存储在RebalanceImpl.processQueueTable队列变量中。
     * <p>
     * 在拉取消息之前会为该Consumer分配消费的消息队列集合mqSet，RebalanceImpl.updateProcessQueueTableInRebalance(String topic, Set<MessageQueue> mqSet)方法的目的是，第一，为该消息队列集合中的每个MessageQueue对象创建一个ProcessQueue对象并存入RebalanceImpl.processQueueTable队列变量中，并将processQueueTable队列中其他的MessageQueue记录删除掉；第二，对于PUSH模式要创建PullRequest对象并放入消息拉取线程（PullMessageService）的pullRequestQueue队列中，由该线程进行消息的拉取处理；大致逻辑如下：
     * <p>
     * 1、遍历RebalanceImpl.processQueueTable:ConcurrentHashMap <MessageQueue, ProcessQueue>集合，检查该集合中的每条记录，对于不在入参MessageQueue集合中记录或者记录中ProcessQueue对象的距离上次拉取时间已经超时，则从该列表中删除掉该记录：
     * <p>
     * 1.1）首先检查MessageQueue的topic值是否等于参数topic；若不相等，则继续遍历下一条记录；若相等，则继续下面的处理逻辑；
     * <p>
     * 1.2) 检查满足第1步的MessageQueue对象是否在入参MessageQueue集合中：若不在此集合中，则将该记录对应的ProcessQueue对象的dropped变量置为true，并且调用 removeUnnecessaryMessageQueue 方法删除该消息队列的消费进度（详见4小节），若删除成功则再将该消息队列的记录从RebalanceImpl.processQueueTable列表中删除并置 changed=true ；若在此集合中，则检查ProcessQueue距离上次拉取时间（ProcessQueue. lastPullTimestamp）是否已经超时（默认120秒），若已经超时，并且消费类型为被动消费（即为PUSH模式，RebalancePushImpl），则将该记录对应的ProcessQueue对象的dropped变量置为true，并且调用 removeUnnecessaryMessageQueue 方法删除该消息队列的消费进度（详见4小节）, 若删除成功则再将遍历到的此条记录从RebalanceImpl.processQueueTable列表中删除并置 changed=true ；
     * <p>
     * 2、遍历入参MessageQueue集合，检查该集合中的每个MessageQueue对象，保证每个MessageQueue对象在processQueueTable队列中都有ProcessQueue对象。对于不在RebalanceImpl.processQueueTable:ConcurrentHashMap <MessageQueue, ProcessQueue>列表中的MessageQueue对象，要构建PullRequest对象以及processQueueTable集合的K-V值，构建过程如下：
     * <p>
     * 2.1）初始化PullRequest对象，并设置consumerGroup、messageQueue变量值， 初始化新的ProcessQueue对象 赋值给PullRequest对象的processQueue变量；
     * <p>
     * 2.2）调用RebalanceImpl. computePullFromWhere (MessageQueue mq)方法获取该MessageQueue对象的下一个消费偏移值offset，详见5小节；
     * <p>
     * 2.3）若该offset值大于等于0，则首先用该offset值设置PullRequest对象的nextOffset值；然后将该PullRequest对象放入局部变量pullRequestList:List<PullRequest>集合中；再将遍历到的MessageQueue对象和该PullRequest对象的processQueue变量存入RebalanceImpl.processQueueTable变量中；最后置 change等于true ；
     * <p>
     * 3、以上一步的局部变量pullRequestList集合为参数调用RebalanceImpl.dispatchPullRequest(List<PullRequest> pullRequestList)方法。该方法的目的是将拉取消息的请求对象PullRequest放入拉取消息的服务线程（PullMessageService）中，并在后台执行拉取消息的业务逻辑。对于PULL模式，是由应用层触发拉取消息，故在RebalancePullImpl子类中该方法没有业务逻辑；对于子类RebalancePushImpl的dispatchPullRequest方法，大致逻辑如下：
     * <p>
     * 遍历入参List<PullRequest>列表，以每个PullRequest对象为参数调用DefaultMQPushConsumerImpl.executePullRequestImmediately(PullRequest pullRequest)方法，在该方法中只调用MQClientInstance.pullMessageService.executePullRequestImmediately(PullRequest pullRequest)方法，将PullRequest对象放入PullMessageService服务线程的pullRequestQueue队列中；在启动Consumer时启动了PullMessageService服务线程，由该服务线程在后台监听pullRequestQueue队列的数据，若队列中有数据到来则执行相应的业务逻辑（详见4小节）；
     * <p>
     * 4、返回 change 的值，该值在进入updateProcessQueueTableInRebalance方法时初始化为false，若在上述步骤中没有重置则返回false；
     *
     * @param topic
     * @param mqSet
     * @param isOrder
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
                                                       final boolean isOrder) {
        boolean changed = false;

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true);
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                        consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                                             final Set<MessageQueue> mqDivided);

    /**
     * 删除未使用的消息队列的消费进度
     * <p>
     * 该方法主用目的是：首先将指定MessageQueue消息队列的消费进度向Broker同步；然后从本地删除该消息队列的消费进度；最后对于PUSH模式下面的顺序消费从Broker端将消息队列解锁。
     * <p>
     * 一、对于PUSH消费模式，即调用RebalancePushImpl.removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)方法，大致逻辑如下：
     * <p>
     * 1、调用DefaultMQPushConsumerImpl.offsetStore.persist(MessageQueue mq)方法：对于广播模式下offsetStore初始化为LocalFileOffsetStore对象，该对象的persist方法没有处理逻辑；对于集群模式下offsetStore初始化为RemoteBrokerOffsetStore对象，该对象的persist方法中，首先以入参MessageQueue对象为key值从RemoteBrokerOffsetStore.offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>变量中获取偏移量offset值，然后调用updateConsumeOffsetToBroker(MessageQueue mq, long offset)方法向Broker发送UPDATE_CONSUMER_OFFSET请求码，向Broker进行消费进度信息的同步；
     * <p>
     * 2、调用DefaultMQPushConsumerImpl.offsetStore.removeOffset(MessageQueue mq)方法：对于广播模式下offsetStore初始化为LocalFileOffsetStore对象，该对象的removeOffset方法没有处理逻辑；对于集群模式下offsetStore初始化为RemoteBrokerOffsetStore对象，该对象的removeOffset方法中，将该MessageQueue对象的记录中从RemoteBrokerOffsetStore.offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>集合中删除;
     * <p>
     * 3、若DefaultMQPushConsumerImpl.consumeOrderly变量等于true（顺序消费类型），并且RebalanceImpl.messageModel变量等于CLUSTERING（集群）,则：首先尝试在1秒内获取到ProcessQueue.lockConsume:Lock的互斥锁；若获取到互斥锁则向该MessageQueue对象的BrokerName下面的主用Broker发送UNLOCK_BATCH_MQ请求码请求对MessageQueue进行解锁操作，采用oneway形式发送请求消息，发送成功则直接返回true，最后释放此互斥锁；若未获取到该lockConsume的互斥锁，则打印试图解锁的次数并返回false。在此使用ProcessQueue.lockConsume互斥锁的原因是保证在发起解锁的过程中没有线程在消费该消息队列（在回调应用层的MessageListenerOrderly实现类的consumeMessage方法时会获取该ProcessQueue.lockConsume互斥锁，与解锁形成互斥逻辑）。
     * <p>
     * 二、对于PULL消费模式，即调用RebalancePullImpl.removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq)方法，大致逻辑与PUSH消费模式的第1/2步逻辑一样，只是没有释放分布式锁的处理逻辑，如下：
     * <p>
     * 1、调用DefaultMQPullConsumerImpl.offsetStore.persist(MessageQueue mq)方法：对于广播模式下offsetStore初始化为LocalFileOffsetStore对象，该对象的persist方法没有处理逻辑；对于集群模式下offsetStore初始化为RemoteBrokerOffsetStore对象，该对象的persist方法中，首先以入参MessageQueue对象为key值从RemoteBrokerOffsetStore.offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>变量中获取偏移量offset值，然后调用updateConsumeOffsetToBroker(MessageQueue mq, long offset)方法向Broker发送UPDATE_CONSUMER_OFFSET请求码，向Broker进行消费进度信息的同步；
     * <p>
     * 2、调用DefaultMQPullConsumerImpl.offsetStore.removeOffset(MessageQueue mq)方法：对于广播模式下offsetStore初始化为LocalFileOffsetStore对象，该对象的removeOffset方法没有处理逻辑；对于集群模式下offsetStore初始化为RemoteBrokerOffsetStore对象，该对象的removeOffset方法中，将该MessageQueue对象的记录中从RemoteBrokerOffsetStore.offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>集合中删除;
     * <p>
     * 3、返回true；
     *
     * @param mq
     * @param pq
     * @return
     */
    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);


    /**
     * RebalanceImpl.computePullFromWhere(MessageQueue mq)方法获取该MessageQueue对象的下一个进度消费值offset。
     * @param mq
     * @return
     */
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
