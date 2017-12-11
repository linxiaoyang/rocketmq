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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

public class PullAPIWrapper {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }


    /**
     * 1、调用PullAPIWrapper.processPullResult(MessageQueue mq,PullResult
     * <p>
     * pullResult, SubscriptionData subscriptionData)方法处理拉取消息的返回对象PullResult，大致逻辑如下：
     * <p>
     * 1.1）调用PullAPIWrapper.updatePullFromWhichNode(MessageQueue mq, long brokerId)方法用Broker返回的PullResultExt.suggestWhichBrokerId变量值更新PullAPIWrapper.pullFromWhichNodeTable:ConcurrentHashMap <MessageQueue,AtomicLong/* brokerId >变量中当前拉取消息PullRequest.messageQueue对象对应的BrokerId。若以messageQueue为key值从pullFromWhichNodeTable中获取的BrokerId为空则将PullResultExt.suggestWhichBrokerId存入该列表中，否则更新该MessageQueue对应的value值为suggestWhichBrokerId；
     * <p>
     * 1.2）若pullResult.status=FOUND，则继续下面的处理逻辑，否则设置PullResultExt.messageBinary=null并返回该PullResult对象；
     * <p>
     * 1.3）对PullResultExt.messageBinary变量进行解码，得到MessageExt列表；
     * <p>
     * 1.4) Consumer端消息过滤。若SubscriptionData.tagsSet集合（在5.5.1小节中拉取消息之前以topic获取的订阅关系数据）不为空并且SubscriptionData. classFilterMode为false（在初始化DefaultMQPushConsumer时可以设置这两个值），则遍历MessageExt列表，检查每个MessageExt对象的tags值（在commitlog数据的properties字段的"TAGS"属性值）是否在SubscriptionData.tagsSet集合中，只保留MessageExt.tags此tagsSet集合中的MessageExt对象，构成新的MessageExt列表，取名msgListFilterAgain；否则新的列表msgListFilterAgain等于上一步的MessageExt列表；
     * <p>
     * Consumer收到过滤后的消息后，同样也要执行在Broker端的操作，但是比对的是真实的Message Tag字符串，而不是hashCode。因为在Broker端为了节约空间，过滤规则是存储的HashCode，为了避免Hash冲突而受到错误消息，在Consumer端还进行一次具体过滤规则的过滤，进行过滤修正。
     * <p>
     * 1.5）检查PullAPIWrapper.filterMessageHookList列表是否为空（可在应用层通过DefaultMQPullConsumerImpl.registerFilterMessageHook (FilterMessageHook hook)方法设置），若不为空则调用该列表中的每个FilterMessageHook对象的filterMessage方法；由应用层实现FilterMessageHook接口的filterMessage方法，可以在该方法中对消息再次过滤；
     * <p>
     * 1.6）向NameServer发送GET_KV_CONFIG请求码获取NAMESPACE_PROJECT_CONFIG和本地IP下面的value值，赋值给projectGroupPrefix变量。若该值为空则将PullResult.minOffset和PullResult.maxOffset值设置到每个MessageExt对象的properties属性中，其中属性名称分别为"MIN_OFFSET"和"MAX_OFFSET"；若不为空则除了将PullResult.minOffset和PullResult.maxOffset值设置到每个MessageExt对象的properties属性中之外，还从projectGroupPrefix变量值开头的topic中去掉projectGroupPrefix值部分，然后将新的topic设置到MessageQueue、SubscriptionData的topic以及每个MessageExt对象的topic变量；
     * <p>
     * 1.7）将新组建的MessageExt列表msgListFilterAgain赋值给PullResult.msgFoundList变量；
     * <p>
     * 1.8）设置PullResultExt.messageBinary=null，并返回该PullResult对象；
     * <p>
     * 2、下面根据PullResult.status变量的值执行不同的业务逻辑，若PullResult.status=FOUND，大致逻辑如下：
     * <p>
     * 2.1）该PullRequest对象的nextOffset变量值表示本次消费的开始偏移量，赋值给临时变量prevRequestOffset；
     * <p>
     * 2.2）取PullResult.nextBeginOffset的值（Broker返回的下一次消费进度的偏移值）赋值给PullRequest.nextOffset变量值；
     * <p>
     * 2.3）若PullResult.MsgFoundList列表为空，则调用DefaultMQPushConsumerImpl.executePullRequestImmediately(PullRequest pullRequest)方法将该拉取请求对象PullRequest重新延迟放入PullMessageService线程的pullRequestQueue队列中，然后跳出该onSucess方法；否则继续下面的逻辑；
     * <p>
     * 2.4）调用该PullRequest.ProcessQueue对象的putMessage(List<MessageExt> msgs)方法，将MessageExt列表存入ProcessQueue.msgTreeMap:TreeMap<Long, MessageExt>变量中，放入此变量的目的是：第一在顺序消费时从该变量列表中取消息进行消费，第二可以用此变量中的消息做流控；大致逻辑如下：
     * <p>
     * A）遍历List<MessageExt>列表，以每个MessageExt对象的queueOffset值为key值，将MessageExt对象存入msgTreeMap:TreeMap<Long, MessageExt>变量中；该变量类型根据key值大小排序；
     * <p>
     * B）更新ProcessQueue.msgCount变量，记录消息个数；
     * <p>
     * C）经过第A步处理之后，若msgTreeMap变量不是空并且ProcessQueue.consuming为false（初始化为false）则置consuming为true（在该msgTreeMap变量消费完之后再置为false）、置临时变量dispatchToConsume为true；否则置临时变量dispatchToConsume为false表示没有待消费的消息或者msgTreeMap变量中存入了数据还未消费完，在没有消费完之前不允许在此提交消费请求，在消费完msgTreeMap之后置consuming为false；
     * <p>
     * D）取List<MessageExt>列表的最后一个MessageExt对象，该对象的properties属性中取MAX_OFFSET的值，减去该MessageExt对象的queueOffset值，即为Broker端该topic和queueId下消息队列中未消费的逻辑队列的大小，将该值存入ProcessQueue.msgAccCnt变量，用于MQClientInstance. adjustThreadPool()方法动态调整线程池大小（在MQClientInstance中启动定时任务定时调整线程池大小）；
     * <p>
     * E）返回临时变量dispatchToConsume值；
     * <p>
     * 2.5）调用ConsumeMessageService.submitConsumeRequest(List <MessageExt> msgs,ProcessQueue processQueue,MessageQueue messageQueue, boolean dispathToConsume)方法，其中dispathToConsume的值由上一步所得,在顺序消费时使用，为true表示可以消费；大致逻辑如下：
     * <p>
     * A）若是 顺序消费 ，则调用ConsumeMessageOrderlyService. submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume)方法；在该方法中，若上次的msgTreeMap变量中的数据还未消费完（即在2.4步中返回dispathToConsume=false）则不执行任何逻辑；若dispathToConsume=true（即上一次已经消费完了）则以ProcessQueue和MessageQueue对象为参数初始化ConsumeMessageOrderlyService类的内部线程类ConsumeRequest；然后将该线程类放入ConsumeMessageOrderlyService.consumeExecutor线程池中。从而可以看出顺序消费是从ProcessQueue对象的TreeMap树形列表中取消息的。
     * <p>
     * B）若是 并发消费 ，则调用ConsumeMessageConcurrentlyService.submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispatchToConsume)方法；在该方法中，则根据批次中最大信息条数（由DefaultMQPushConsumer.consumeMessageBatchMaxSize设置,默认为1）来决定是否分提交到线程池中，大致逻辑为:首先比较List<MessageExt>列表的个数是否大于了批处理的最大条数，若没有则以该List<MessageExt>队列、ProcessQueue对象、MessageQueue对象初始化ConsumeMessageConcurrentlyService的内部类 ConsumeRequest 的对象，并放入ConsumeMessageConcurrentlyService.consumeExecutor线程池中；否则遍历List<MessageExt>列表，每次从List<MessageExt>列表中取consumeMessageBatchMaxSize个MessageExt对象构成新的List<MessageExt>列表，然后以新的MessageExt队列、ProcessQueue对象、MessageQueue对象初始化ConsumeMessageConcurrentlyService的内部类 ConsumeRequest 的对象并放入ConsumeMessageConcurrentlyService.consumeExecutor线程池中，直到该队列遍历完为止。从而可以看出并发消费是将从Broker获取的MessageExt消息列表分配到各个 ConsumeRequest 线程中进行并发消费。
     * <p>
     * 2.6）检查拉取消息的间隔时间（DefaultMQPushConsumer.pullInterval，默认为0），若大于0，则调用DefaultMQPushConsumerImpl. executePullRequestLater方法，在间隔时间之后再将PullRequest对象放入PullMessageService线程的pullRequestQueue队列中；若等于0（表示立即再次进行拉取消息），则调用DefaultMQPushConsumerImpl. executePullRequestImmediately方法立即继续下一次拉取消息，从而形成一个循环不间断地拉取消息的过程；
     * <p>
     * 3、若PullResult.status=NO_NEW_MSG或者NO_MATCHED_MSG时：
     * <p>
     * 3.1）取PullResult.nextBeginOffset的值（Broker返回的下一次消费进度的偏移值）赋值给PullRequest.nextOffset变量值；
     * <p>
     * 3.2）更新消费进度offset。调用DefaultMQPushConsumerImpl.correctTagsOffset(PullRequest pullRequest)方法。若没有获取到消息（即ProcessQueue.msgCount等于0）则更新消息进度。对于LocalFileOffsetStore或RemoteBrokerOffsetStore类，均调用updateOffset(MessageQueue mq, long offset, boolean increaseOnly)方法，而且方法逻辑是一样的，以MessageQueue对象为key值从offsetTable:ConcurrentHashMap<MessageQueue, AtomicLong>变量中获取values值，若该values值为空，则将MessageQueue对象以及offset值（在3.1步中获取的PullResult.nextBeginOffset值）存入offsetTable变量中，若不为空，则比较已经存在的值，若大于已存在的值才更新；
     * <p>
     * 3.3）调用DefaultMQPushConsumerImpl.executePullRequestImmediately方法立即继续下一次拉取；
     * <p>
     * 4、若PullResult.status=OFFSET_ILLEGAL
     * <p>
     * 4.1）取PullResult.nextBeginOffset的值（Broker返回的下一次消费进度的偏移值）赋值给PullRequest.nextOffset变量值；
     * <p>
     * 4.2）设置PullRequest.processQueue.dropped等于true，将此该拉取请求作废；
     * <p>
     * 4.3）创建一个匿名Runnable线程类，然后调用DefaultMQPushConsumerImpl.executeTaskLater(Runnable r, long timeDelay)方法将该线程类放入PullMessageService.scheduledExecutorService: ScheduledExecutorService调度线程池中，在10秒钟之后执行该匿名线程类；该匿名线程类的run方法逻辑如下：
     * <p>
     * A）调用OffsetStore.updateOffset(MessageQueue mq, long offset, boolean increaseOnly)方法更新更新消费进度offset；
     * <p>
     * B）调用OffsetStore.persist(MessageQueue mq)方法：对于广播模式下offsetStore初始化为LocalFileOffsetStore对象，该对象的persist方法没有处理逻辑；对于集群模式下offsetStore初始化为RemoteBrokerOffsetStore对象，该对象的persist方法中，首先以入参MessageQueue对象为key值从RemoteBrokerOffsetStore.offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>变量中获取偏移量offset值，然后调用updateConsumeOffsetToBroker(MessageQueue mq, long offset)方法向Broker发送UPDATE_CONSUMER_OFFSET请求码的消费进度信息；
     * C）以PullRequest对象的messageQueue变量为参数调用RebalanceImpl.removeProcessQueue(MessageQueue mq)方法，在该方法中，首先从RebalanceImpl.processQueueTable: ConcurrentHashMap<MessageQueue, ProcessQueue>变量中删除MessageQueue记录并返回对应的ProcessQueue对象；然后该ProcessQueue对象的dropped变量设置为ture；最后以MessageQueue对象和ProcessQueue对象为参数调用removeUnnecessaryMessageQueue方法删除未使用的消息队列的消费进度，具体逻辑详见5.3.4小节；
     *
     * @param mq
     * @param pullResult
     * @param subscriptionData
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * 拉取消息的底层API接口
     * <p>
     * <p>
     * PUSH模式和PULL模式下的拉取消息的操作均调用PullAPIWrapper.pullKernelImpl(MessageQueue mq, String subExpression, long subVersion,long offset, int maxNums, int sysFlag,long commitOffset,long brokerSuspendMaxTimeMillis, long timeoutMillis, CommunicationMode communicationMode, PullCallback pullCallback)方法完成。该方法的大致逻辑如下：
     * <p>
     * 1、获取Broker的ID。以入参MessageQueue对象为参数调用PullAPIWrapper.recalculatePullFromWhichNode(MessageQueue mq)方法，在该方法中，先判断PullAPIWrapper.connectBrokerByUser变量是否为true（在FiltersrvController中启动时会设置为true，默认为false），若是则直接返回0（主用Broker的brokerId）；否则以MessageQueue对象为参数从PullAPIWrapper.pullFromWhichNodeTable:ConcurrentHashMap<MessageQueue, AtomicLong/* brokerId >获取brokerId，若该值不为null则返回该值，否则返回0（主用Broker的brokerId）；
     * <p>
     * 2、调用MQClientInstance.findBrokerAddressInSubscribe(String brokerName ,long brokerId,boolean onlyThisBroker) 方法查找Broker地址，其中onlyThisBroker=false，表示若指定的brokerId未获取到地址则获取其他BrokerId的地址也行。在该方法中根据brokerName和brokerId参数从MQClientInstance.brokerAddrTable: ConcurrentHashMap<String/* Broker Name , HashMap<Long/* brokerId , String/* address >>变量中获取对应的Broker地址，若获取不到则从brokerName下面的Map列表中找其他地址返回即可；
     * <p>
     * 3、若在上一步未获取到Broker地址，则以topic参数调用MQClientInstance.updateTopicRouteInfoFromNameServer(String topic)方法，然后在执行第2步的操作，直到获取到Broker地址为止；
     * <p>
     * 4、若获取的Broker地址是备用Broker，则将标记位sysFlag的第1个字节置为0，即在消费完之后不提交消费进度；
     * <p>
     * 5、检查标记位sysFlag的第4个字节（即SubscriptionData. classFilterMode）是否为1；若等于1，则调用PullAPIWrapper.computPullFromWhichFilterServer(String topic, String brokerAddr)方法获取Filter服务器地址。大致逻辑如下：
     * <p>
     * 5.1)根据topic参数值从MQClientInstance.topicRouteTable: ConcurrentHashMap<String/*Topic,TopicRouteData>变量中获取TopicRouteData对象，
     * <p>
     * 5.2)以Broker地址为参数从该TopicRouteData对象的filterServerTable:HashMap<String/* brokerAddr,List<String>/* FilterServer>变量中获取该Broker下面的所有Filter服务器地址列表；
     * <p>
     * 5.3)若该地址列表不为空，则随机选择一个Filter服务器地址返回；否则向调用层抛出异常，该pullKernelImpl方法结束；
     * <p>
     * 6、构建PullMessageRequestHeader对象，其中queueOffset变量值等于入参offset；
     * <p>
     * 7、若执行了第5步则向获取的Filter服务器发送PULL_MESSAGE请求信息，否则向Broker发送PULL_MESSAGE请求信息。初始化PullMessageRequestHeader对象，然后调用MQClientAPIImpl.pullMessage(String addr, PullMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, PullCallback pullCallback)方法向Broker地址或者Filter地址发送PULL_MESSAGE请求信息（详见5.10小节）；
     *
     * @param mq
     * @param subExpression
     * @param expressionType
     * @param subVersion
     * @param offset
     * @param maxNums
     * @param sysFlag
     * @param commitOffset
     * @param brokerSuspendMaxTimeMillis
     * @param timeoutMillis
     * @param communicationMode
     * @param pullCallback
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //找broker
        FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            //如果找不大，就从nameserver上拉一次，然后找
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                    brokerAddr,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pullKernelImpl(
                mq,
                subExpression,
                ExpressionType.TAG,
                subVersion, offset,
                maxNums,
                sysFlag,
                commitOffset,
                brokerSuspendMaxTimeMillis,
                timeoutMillis,
                communicationMode,
                pullCallback
        );
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
