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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

public class DefaultMQPullConsumerImpl implements MQConsumerInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final RPCHook rpcHook;
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private OffsetStore offsetStore;
    private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);

    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer, final RPCHook rpcHook) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.rpcHook = rpcHook;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        this.makeSureStateOK();
        return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }

    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        this.makeSureStateOK();
        if (null == topic) {
            throw new IllegalArgumentException("topic is null");
        }

        ConcurrentMap<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
        Set<MessageQueue> mqResult = new HashSet<MessageQueue>();
        for (MessageQueue mq : mqTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                mqResult.add(mq);
            }
        }

        return mqResult;
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subExpression, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, false, timeout);
    }

    private PullResult pullSyncImpl(MessageQueue mq, String subExpression, long offset, int maxNums, boolean block,
                                    long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);

        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

        SubscriptionData subscriptionData;
        try {
            subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                    mq.getTopic(), subExpression);
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }

        long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
                mq,
                subscriptionData.getSubString(),
                0L,
                offset,
                maxNums,
                sysFlag,
                0,
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        if (!this.consumeMessageHookList.isEmpty()) {
            ConsumeMessageContext consumeMessageContext = null;
            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setConsumerGroup(this.groupName());
            consumeMessageContext.setMq(mq);
            consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
            consumeMessageContext.setSuccess(false);
            this.executeHookBefore(consumeMessageContext);
            consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            consumeMessageContext.setSuccess(true);
            this.executeHookAfter(consumeMessageContext);
        }
        return pullResult;
    }

    public void subscriptionAutomatically(final String topic) {
        if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
            try {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                        topic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
            } catch (Exception ignore) {
            }
        }
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    @Override
    public String groupName() {
        return this.defaultMQPullConsumer.getConsumerGroup();
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> result = new HashSet<SubscriptionData>();

        Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
        if (topics != null) {
            synchronized (topics) {
                for (String t : topics) {
                    SubscriptionData ms = null;
                    try {
                        ms = FilterAPI.buildSubscriptionData(this.groupName(), t, SubscriptionData.SUB_ALL);
                    } catch (Exception e) {
                        log.error("parse subscription error", e);
                    }
                    ms.setSubVersion(0L);
                    result.add(ms);
                }
            }
        }

        return result;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPullConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        pull(mq, subExpression, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback,
                     long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, false, timeout);
    }

    private void pullAsyncImpl(
            final MessageQueue mq,
            final String subExpression,
            final long offset,
            final int maxNums,
            final PullCallback pullCallback,
            final boolean block,
            final long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        if (null == pullCallback) {
            throw new MQClientException("pullCallback is null", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        try {
            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

            final SubscriptionData subscriptionData;
            try {
                subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                        mq.getTopic(), subExpression);
            } catch (Exception e) {
                throw new MQClientException("parse subscription error", e);
            }

            long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

            this.pullAPIWrapper.pullKernelImpl(
                    mq,
                    subscriptionData.getSubString(),
                    0L,
                    offset,
                    maxNums,
                    sysFlag,
                    0,
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                    timeoutMillis,
                    CommunicationMode.ASYNC,
                    new PullCallback() {

                        /**
                         * 在收到响应消息之后，先回调InvokeCallback匿名类的operationComplete (ResponseFuture responseFuture)方法，
                         * 在正常情况下会回调PullCallback类的onSucess方法
                         * @param pullResult
                         */
                        @Override
                        public void onSuccess(PullResult pullResult) {
                            pullCallback
                                    .onSuccess(DefaultMQPullConsumerImpl.this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData));
                        }

                        /**
                         * 在发送消息失败或者等待响应超时或者其他异常时回调该onException方法。在该方法中，调用DefaultMQPushConsumerImpl.executePullRequestLater方法，
                         * 3秒钟之后再将该PullRequest请求重新放入PullMessageService线程的pullRequestQueue队列中
                         * @param e
                         */
                        @Override
                        public void onException(Throwable e) {
                            pullCallback.onException(e);
                        }
                    });
        } catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }

    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, true, this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
                                    PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, true,
                this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumerGroup());
    }

    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            if (UtilAll.isBlank(consumerGroup)) {
                consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();
            }

            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup, delayLevel, 3000,
                    this.defaultMQPullConsumer.getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPullConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(this.defaultMQPullConsumer.getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }


    /**
     *
     *
     * 1、调用DefaultMQPullConsumer.fetchSubscribeMessageQueues(String topic)方法，根据topic获取对应的MessageQueue（即可被订阅的队列），在该方法中最终通过调用MQAdminImpl.fetchSubscribeMessageQueues(String topic)方法从NameServer获取该topic的MessageQueue，大致逻辑如下：

     1.1）调用MQClientAPIImpl.getTopicRouteInfoFromNameServer(String topic, long timeoutMillis)方法，其中timeoutMillis=3000，该方法向NameServer发送GET_ROUTEINTO_BY_TOPIC请求码获取topic参数对应的Broker信息和topic配置信息，即TopicRouteData对象；

     1.2）遍历TopicRouteData对象的QueueData列表中每个QueueData对象，首先判断该QueueData对象是否具有读权限，若有则根据该QueueData对象的readQueueNums值，创建readQueueNums个MessageQueue对象，并构成MessageQueue集合；最后返回给MessageQueue集合；

     1.3)若上一步构建的MessageQueue集合不为空，则返回给该集合，否则抛MQClientException异常；

     2、调用 DefaultMQPullConsumer.pullBlockIfNotFound( MessageQueue mq, String subExpression, long offset, int maxNums)方法获取该 MessageQueue队列下面从offset位置开始的消息内容，其中maxNums=32即表示获取的最大消息个数，offset为该MessageQueue对象的开始消费位置，可以调用DefaultMQPullConsumer.fetchConsumeOffset(MessageQueue mq, boolean fromStore)方法获取该MessageQueue队列的消费进度来设定参数offset值（详见5.6.2小节）；该方法最终调用DefaultMQPullConsumerImpl.pullSyncImpl(MessageQueue mq, String subExpression, long offset, int maxNums, boolean block)方法，该方法的大致逻辑如下：

     2.1）检查MessageQueue对象的topic是否在RebalanceImpl.subscriptionInner:ConcurrentHashMap<String,SubscriptionData>变量中，若不在则以consumerGroup、topic、subExpression为参数调用FilterAPI.buildSubscriptionData(String consumerGroup, String topic, String subExpression)方法构造SubscriptionData对象保存到RebalanceImpl.subscriptionInner变量中，其中 subExpression="*" ；

     2.2）构建消息的标志位sysFlag，其中suspend和subscription为true（即该标记位的第2/3位为1），其他commit和classFilter两位为false（第1/4位为0）；

     2.3）以请求参数subExpression以及consumerGroup、topic为参数调用FilterAPI.buildSubscriptionData(String consumerGroup,Stringtopic, String subExpression)方法构造SubscriptionData对象并返回；

     2.4）调用PullAPIWrapper.pullKernelImpl(MessageQueue mq, String subExpression, long subVersion, long offset, int maxNums, int sysFlag, long commitOffset, long brokerSuspendMaxTimeMillis, long timeoutMillis, CommunicationMode communicationMode, PullCallback pullCallback)方法从Broker拉取消息内容；

     2.5）调用PullAPIWrapper.processPullResult(MessageQueue mq, PullResult pullResult, SubscriptionData subscriptionData)方法对拉取消息的响应结果进行处理，主要是消息反序列化；（详细逻辑见 5.5.2小节—内部匿名类PullCallback的onSuccess方法部分）

     3、在本地创建offseTable:Map<MessageQueue, Long>变量，根据上一步返回的结果，将该MessageQueue的下一次消费开始位置记录下来；
     *
     *
     *
     * @throws MQClientException
     */
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                this.copySubscription();

                if (this.defaultMQPullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPullConsumer.changeInstanceNameToPID();
                }

                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPullConsumer, this.rpcHook);

                this.rebalanceImpl.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPullConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPullConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPullConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPullConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPullConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPullConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPullConsumer.setOffsetStore(this.offsetStore);
                }

                this.offsetStore.load();

                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;

                    throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

    }

    private void checkConfig() throws MQClientException {
        // check consumerGroup
        Validators.checkGroup(this.defaultMQPullConsumer.getConsumerGroup());

        // consumerGroup
        if (null == this.defaultMQPullConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumerGroup
        if (this.defaultMQPullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // messageModel
        if (null == this.defaultMQPullConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // allocateMessageQueueStrategy
        if (this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    private void copySubscription() throws MQClientException {
        try {
            Set<String> registerTopics = this.defaultMQPullConsumer.getRegisterTopics();
            if (registerTopics != null) {
                for (final String topic : registerTopics) {
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                            topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.makeSureStateOK();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    public void setPullAPIWrapper(PullAPIWrapper pullAPIWrapper) {
        this.pullAPIWrapper = pullAPIWrapper;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long getConsumerStartTimestamp() {
        return consumerStartTimestamp;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}
