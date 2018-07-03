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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;

public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final Logger log = ClientLogger.getLog();
    private final ClientConfig clientConfig;
    private final int instanceIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    /**
     * tocic 与 路由数据的map
     * <p>
     * TopicRouteData
     * <p>
     * List<QueueData> queueDatas
     * List<BrokerData> brokerDatas
     */
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    /**
     * org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)
     * 添加的数据
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();

    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
            new ConcurrentHashMap<String, HashMap<String, Integer>>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });
    private final ClientRemotingProcessor clientRemotingProcessor;
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private final ConsumerStatsManager consumerStatsManager;
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private DatagramSocket datagramSocket;
    private Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}",
                this.instanceIndex,
                this.clientId,
                this.clientConfig,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }


    /**
     * 调用MQClientInstance.start方法启动MQClientInstance对象；大致逻辑如下：
     * <p>
     * 1、检查MQClientInstance.ServiceState的状态（初始化状态为ServiceState.CREATE_JUST）；只有状态为CREATE_JUST时才启动该Producer；其他状态均不执行启动过程；
     * <p>
     * 2、将MQClientInstance的ServiceState置为start_failed,以免重复启动；
     * <p>
     * 3、检查是否设置了NameServer的地址（在启动Producer之前在应用层设置ClientConfig.namesrvAddr），若没有则从地址服务器找Name Server地址，地址服务器的路径是"http://&quot; +WS_DOMAIN_NAME+":8080/rocketmq/"+WS_DOMAIN_SUBGROUP；其中WS_DOMAIN_NAME为system.property配置参数rocketmq.namesrv.domain的值，WS_DOMAIN_SUBG为system.property配置参数rocketmq.namesrv.domain.subgroup的值；
     * <p>
     * 4、调用MQClientAPIImpl.start方法：首先调用MQClientAPIImpl. remotingClient方法，启动Netty客户端；然后以PROJECT_CONFIG为NameSpace，本地IP为key值向NameServer发送GET_KV_CONFIG请求，获取前缀赋值给MQClientAPIImpl.projectGroupPrefix变量；
     * <p>
     * 5、设置各类定时任务：
     * <p>
     * 5.1）若Name Server地址（ClientConfig.namesrvAddr）为空，则设置定时获取Name Server地址，每隔120秒从地址服务器查找Name Server地址；
     * <p>
     * 5.2）每隔30秒调用一次MQClientInstance.updateTopicRouteInfoFromNameServer()方法更新topic的路由信息；
     * <p>
     * 5.3）每隔30秒进行一次清理离线的Broker的工作。遍历MQClientInstance.brokerAddrTable的值，对于不在MQClientInstance.topicRouteTable中的Broker地址，从brokerAddrTable中清理掉；
     * <p>
     * 5.4）每隔30秒调用一次MQClientInstance.sendHeartbeatToAllBrokerWithLock()方法，在该方法中，第一，向所有在MQClientInstance.brokerAddrTable列表中的Broker发送心跳消息；第二，向Filter过滤服务器发送REGISTER_MESSAGE_FILTER_CLASS请求码，更新过滤服务器中的Filterclass文件；
     * <p>
     * 5.5）每隔5秒调用一次MQClientInstance.persistAllConsumerOffset()方法。将每个Consumer端的消费进度信息向Broker同步（详细步骤见4.2.2小节），只有Consumer端才会进行同步；Producer端不发起同步；
     * <p>
     * 5.6）定期每1毫秒执行一次MQClientInstance.adjustThreadPool()方法检测并调整PUSH模式（DefaultMQPushConsumer）下ConsumeMessageService对象中线程池的线程数）。该ConsumeMessageService类有两个子类ConsumeMessageConcurrentlyService和ConsumeMessageOrderlyService，分别表示并发消费和顺序消费两类情况，这两个之类的adjustThreadPool方法是一样的，在此，以ConsumeMessageConcurrentlyService对象为例，在初始化ConsumeMessageConcurrentlyService对象的过程中利用DefaultMQPushConsumer.consumeThreadMax和DefaultMQPushConsumer. consumeThreadMin初始化consumeExecutor线程池。在该方法中遍历MQClientInstance.consumerTable:ConcurrentHashMap<String/* group , MQConsumerInner>变量，若MQConsumerInner为DefaultMQPushConsumerImpl，则调用DefaultMQPushConsumerImpl.adjustThreadPool()方法，在该方法中，首先汇总DefaultMQPushConsumerImpl.RebalanceImpl.processQueueTable中所有ProcessQueue对象的msgAccCnt值（即表示Broker端未消费的逻辑队列大小）；由DefaultMQPushConsumer.adjustThreadPoolNumsThreshold参数设置阀值，默认为100000；
     * <p>
     * 若汇总的msgAccCnt值大于adjustThreadPoolNumsThreshold值，并且ConsumeMessageConcurrentlyService.consumeExecutor的当前线程数量小于最大线程数，则将当前线程数量加1；
     * <p>
     * 若汇总的msgAccCnt值小于adjustThreadPoolNumsThreshold*0.8，并且ConsumeMessageConcurrentlyService.consumeExecutor的当前线程数量大于最小线程数，则将当前线程数量减1；
     * <p>
     * 6、启动PullMessageService服务线程，该服务监听PullMessageService.pullRequestQueue：LinkedBlockingQueue<PullRequest>队列，若该队列有拉取消息的请求，则选择Consumer进行消息的拉取，该定时服务是Consumer使用的；
     * <p>
     * 7、启动RebalanceService服务线程，该服务是Consumer端使用的；
     * <p>
     * 8、启动在初始化MQClientInstance对象过程中初始化的DefaultMQProducer对象，即调用内部初始化的DefaultMQProducer对象的DefaultMQProducerImpl.start（false）方法，参数为false表示在启动该Producer对象的内部不启动MQClientInstance；
     * <p>
     * 9、设置MQClientInstance的ServiceState为RUNNING；
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    // TODO 启动远程连接的client,基于netty同服务端建立好连接，等待发送数据,待细细观察
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    // TODO 开始各种定时任务，待细细观察
                    this.startScheduledTask();
                    // Start pull service
                    // TODO 开始拉取的服务，是一个定时任务
                    this.pullMessageService.start();
                    // Start rebalance service
                    this.rebalanceService.start();
                    // Start push service
                    // TODO 开始推送服务，也就是发送消息，有点像product与consumer公用一个
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }


    /**
     * 方法的目的主要有：
     * <p>
     * 1）从NameServer获取该客户端订阅的每个topic的相关信息（包括每个topic对应的Broker信息和topic配置信息），用TopicRouteData对象表示；并将该对象信息存入MQClientInstance.topicRouteTable变量中；
     * <p>
     * 2）根据返回的信息更新topic对应的BrokerName和BrokerId，保存在变量 MQClientInstance.brokerAddrTable中；
     * <p>
     * 3）对于Prodcuer，根据每个topic以及配置的写队列个数创建MessageQueue消费队列，并存入变量DefaultMQProducerImpl.topicPublishInfoTable中；
     * <p>
     * 4）对于已经订阅了该topic的Consumer，根据每个topic以及配置的读队列个数创建MessageQueue消费队列，并存入变量RebalanceImpl. topicSubscribeInfoTable中；
     * <p>
     * 大致逻辑如下：
     * <p>
     * 1、获取Producer或Consumer端的所有topic列表，步骤如下：
     * <p>
     * 1.1）对于PushConsumer端：遍历MQClientInstance.consumerTable:
     * <p>
     * ConcurrentHashMap<String/* group , MQConsumerInner>变量，对于每个DefaultMQPushConsumerImpl（MQConsumerInner的子类），获取DefaultMQPushConsumerImpl.rebalanceImpl:RebalanceImpl变量中的RebalanceImpl.subscriptionInner:ConcurrentHashMap<String, SubscriptionData>变量的values值，即SubscriptionData的集合；然后遍历SubscriptionData集合，取出每个SubscriptionData对象的topic变量值，即构成了该PushConsumer端的所有topic列表；
     * <p>
     * 1.2）对于PullConsumer端：遍历MQClientInstance.consumerTable:
     * <p>
     * ConcurrentHashMap<String/* group , MQConsumerInner>变量，对于每个DefaultMQPullConsumerImpl（MQConsumerInner的子类），对应的DefaultMQPullConsumer.registerTopics的变量值集合构成了该PullConsumer端的所有topic列表；
     * <p>
     * 1.3）对于Producer端：遍历MQClientInstance.producerTable: ConcurrentHashMap<String/* group , MQProducerInner>变量，DefaultMQProducerImpl为MQProducerInner的子类，获取每个DefaultMQProducerImpl.topicPublishInfoTable:ConcurrentHashMap<String/* topic , TopicPublishInfo>变量的key值集合，即构成了Producer端的topic列表；对于刚启动的Producer，该topic列表只有默认值"TBW102"；
     * <p>
     * 2、遍历上一步获取的topic列表，以每个topic为参数调用MQClientInstance.updateTopicRouteInfoFromNameServer(String topic)方法，在该方法中调用updateTopicRouteInfoFromNameServer(String topic, boolean isDefault, DefaultMQProducer defaultMQProducer)方法，其中isDefault=false，defaultMQProducer=null，大致步骤如下：
     * <p>
     * 2.1）在此调用中请求参数isDefault=false，defaultMQProducer=null，则以参数topic向NameServer发送GET_ROUTEINTO_BY_TOPIC 请求码的请求从NameServer获取对应的Broker信息和topic配置信息，即返回TopicRouteData对象。但是若isDefault=true并且defaultMQProducer不等于null，则以主题名"TBW102"为topic向NameServer发送GET_ROUTEINTO_BY_TOPIC 请求码的请求从NameServer获取对应的Broker信息和topic配置信息，即返回TopicRouteData对象。
     * <p>
     * 2.2）若第2.1步获取的TopicRouteData对象为空则直接返回，否则继续下面的操作；根据topic参数值从MQClientInstance.topicRouteTable本地列表中获取TopicRouteData对象；
     * <p>
     * 2.3）比较从本地列表获取的TopicRouteData对象与从NameServer获取获取的TopicRouteData对象是否相等，若不相等则表示要更新topic信息，继续执行第2.4步操作；若相等则再检查MQClientInstance.consumerTable或者producerTable变量中每个Producer或Consumer端是否都包含该topic的订阅信息或者订阅信息中的MessageQueue队列是否为空，若没有订阅信息或者订阅信息中的队列为空则返回true并继续执行2.4步的操作；
     * <p>
     * A）对于Producer端：遍历MQClientInstance.producerTable: ConcurrentHashMap<String/* group , MQProducerInner>变量中的每个DefaultMQProducerImpl对象，调用DefaultMQProducerImpl对象的isPublishTopicNeedUpdate(String topic)方法，在该方法中，以topic值获取DefaultMQProducerImpl.topicPublishInfoTable:ConcurrentHashMap<String/* topic , TopicPublishInfo>变量的TopicPublishInfo对象，若遇到获取的该对象为null或者该对象的messageQueueList:List<MessageQueue>列表变量为空则跳出该遍历过程，并返回true，表示需要更新topic信息；
     * <p>
     * B）对于PushConsumer端：遍历MQClientInstance.consumerTable:
     * <p>
     * ConcurrentHashMap<String/* group , MQConsumerInner>变量中的每个DefaultMQPushConsumerImpl对象，首先调用DefaultMQPushConsumerImpl对象的isSubscribeTopicNeedUpdate(String topic)方法，在该方法中获取DefaultMQPushConsumerImpl.rebalanceImpl: RebalanceImpl变量中的RebalanceImpl.subscriptionInner:ConcurrentHashMap<String/*topic, SubscriptionData>变量，检查该Consumer是否订阅了此topic（在Consumer启动时设置该变量的值），即该topic是否再该subscriptionInner变量中，若不在则继续遍历其他的DefaultMQPushConsumerImpl对象直到遍历完为止；若在subscriptionInner变量中则再检查该topic是否在RebalanceImpl.topicSubscribeInfoTable: ConcurrentHashMap<String/*topic, Set<MessageQueue>>变量中；若不在则表示该topic有订阅关系但是没有生成MessageQueue列表，则返回true（表示需要更新topic信息）并且跳出此遍历过程。
     * <p>
     * C）对于PullConsumer端的判断逻辑与PushConsumer端是一样的；
     * <p>
     * 2.4）遍历从NameServer获取的TopicRouteData对象的BrokerDatas变量，将每个BrokerData对象的BrokerName和BrokerAddr:HashMap<Long/*brokerId , String/* broker address>为key、values值存入 MQClientInstance.brokerAddrTable: ConcurrentHashMap<String/*BrokerName,HashMap<Long/*brokerId, String/*address>>变量中 ；
     * <p>
     * 2.5）更新Producer端的topic信息：
     * <p>
     * A）调用MQClientInstance.topicRouteData2TopicPublishInfo(String topic, TopicRouteData route)方法创建TopicPublishInfo对象：
     * A.1）若TopicRouteData对象的orderTopicConf不为空并且长度大于零，则以";"解析成数组，数组的每个成员是以":"分隔的，结构是"brokerName:queueNum"；遍历数组的每个成员变量，为每个成员变量创建queueNum个MessageQueue对象，存入TopicPublishInfo.messageQueueList: List<MessageQueue>变量中；遍历结束之后将TopicPublishInfo.orderTopic设置为true；
     * A.2）若TopicRouteData对象的orderTopicConf为空，则取TopicRouteData对象的QueueData列表；然后遍历该QueueData列表的每个QueueData对象：
     * <p>
     * A.2.1）检查该QueueData对象是否具有写权限；若没有则继续遍历下一个QueueData对象；
     * A.2.2）对于QueueData对象的BrokerName值，若该值等于TopicRouteData对象的BrokerData列表中某一BrokerData对象的BrokerName值，再检查BrokerData对象的BrokerAddr:HashMap<Long/* brokerId , String/* broker address >列表中是否包含了主用Broker（ID=0）,即检查BrokerName名下是否有主用Broker; 若存在主用Broker，则取该QueueData对象的writeQueueNums值，以该Broker的name、入参topic、从0开始的序号（小于writeQueueNums）为参数初始化writeQueueNums个MessageQueue对象， 存入TopicPublishInfo.messageQueueList:List<MessageQueue>变量中 ；若该BrokerName集群下面没有主用Broker，则不创建写入队列MessageQueue对象；
     * A.2.3）继续遍历下一个QueueData对象,直到遍历完该QueueData列表为止，最后将TopicPublishInfo.orderTopic设置为false；
     * <p>
     * A.3）返回TopicPublishInfo对象；
     * <p>
     * B）设置TopicPublishInfo.haveTopicRouterInfo等于true，表示已经有最新topic信息，即TopicPublishInfo对象；
     * <p>
     * C）遍历MQClientInstance.producerTable: ConcurrentHashMap<String/* group , MQProducerInner>变量，调用每个DefaultMQProducerImpl对象的updateTopicPublishInfo(String topic, TopicPublishInfo info)方法，以该topic为key值、上一步返回的TopicPublishInfo对象为values值 存入该DefaultMQProducerImpl.topicPublishInfoTable变量中；
     * <p>
     * 2.6）更新Consumer端的topic信息：
     * <p>
     * A）构建MessageQueue列表。遍历TopicRouteData对象的QueueData列表中每个QueueData对象，首先判断该QueueData对象是否具有读权限，若有则根据该QueueData对象的readQueueNums值，创建readQueueNums个MessageQueue对象，并构成MessageQueue集合；最后返回该MessageQueue集合；
     * <p>
     * B）对于PushConsumer端和PullConsumer端，处理逻辑是一样的，以DefaultMQPushConsumerImpl为例：遍历MQClientInstance.consumerTable:ConcurrentHashMap<String/* group , MQConsumerInner>中的MQConsumerInner对象；获取DefaultMQPushConsumerImpl.rebalanceImpl:RebalanceImpl变量中的RebalanceImpl.subscriptionInner：ConcurrentHashMap<String, SubscriptionData>变量值；若topic在该subscriptionInner变量值的列表中，则用topic和MessageQueue集合更新RebalanceImpl. topicSubscribeInfoTable:ConcurrentHashMap<String/* topic , Set<MessageQueue>>变量；
     * <p>
     * 2.7）将该topic以及TopicRouteData对象存入MQClientInstance.topicRouteTable:ConcurrentHashMap<String/* Topic ,TopicRouteData>变量中；
     */
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                                addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                    + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                    + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                    "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }


    /**
     * 向Broker同步消费进度的定时任务
     *
     *
     * 每隔5秒调用一次MQClientInstance.persistAllConsumerOffset()方法将消费进度向Broker同步。遍历MQClientInstance.consumerTable: ConcurrentHashMap<String/*group , MQConsumerInner>变量。对于PushConsumer端和PullConsumer端，处理逻辑是一样的,以DefaultMQPushConsumerImpl为例，调用DefaultMQPushConsumerImpl.persistConsumerOffset()方法。
     * 1、获取DefaultMQPushConsumerImpl.rebalanceImpl变量的processQueueTable:ConcurrentHashMap<MessageQueue, ProcessQueue>变量值，取该变量的key值集合，即MessageQueue集合；以该集合为参数调用OffsetStore.persistAll(Set<MessageQueue> mqs)方法；
     * 2、若消息模式是广播（BROADCASTING），即DefaultMQPushConsumerImpl.offsetStore变量初始化为LocalFileOffsetStore对象，在此调用LocalFileOffsetStore.persistAll(Set<MessageQueue> mqs)方法，在此方法中遍历LocalFileOffsetStore.offsetTable:ConcurrentHashMap<MessageQueue,AtomicLong>变量，将包含在第1步的MessageQueue集合中的MessageQueue对象的消费进度持久化到consumerOffset.json物理文件中；
     * 3、若消息模式为集群模式，即DefaultMQPushConsumerImpl.offsetStore变量初始化为RemoteBrokerOffsetStore对象，在此调用RemoteBrokerOffsetStore.persistAll(Set<MessageQueue> mqs)方法，在此方法中遍历RemoteBrokerOffsetStore.offsetTable:ConcurrentHashMap <MessageQueue,AtomicLong>变量；对于包含在第1步的MessageQueue集合中的MessageQueue对象，调用updateConsumeOffsetToBroker(MessageQueuemq, long offset)方法向Broker发送UPDATE_CONSUMER_OFFSET请求码的消费进度信息；
     */
    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * 向Broker发送心跳消息
     * <p>
     * 1、初始化HeartbeatData对象，将该Producer或Consumer的ClientID赋值给HeartbeatData对象的clientID变量；
     * <p>
     * 2、遍历MQClientInstance.consumerTable: ConcurrentHashMap<String/* group , MQConsumerInner>变量，根据每个MQConsumerInner对象的值初始化ConsumerData对象（包括GroupName、consumeType、MessageModel、consumeFromWhere、rebalanceImpl变量中的RebalanceImpl. subscriptionInner值初始化subscriptionDataSet:Set<SubscriptionData>变量、UnitMode），并将该对象放入HeartbeatData对象的ConsumerData集合变量consumerDataSet:Set<ConsumerData>中；
     * <p>
     * 3、遍历MQClientInstance.producerTable: ConcurrentHashMap<String/* group , MQProducerInner>变量；根据每条记录的ProducerGroup值初始化ProducerData对象，并放入HeartbeatData对象的ProducerData集合变量producerDataSet:Set<ProducerData>中；
     * <p>
     * 4、若ConsumerData集合和ProducerData集合都为空，说明没有consumer或produer，则不发送心跳信息；
     * <p>
     * 5、若不是都为空，则遍历MQClientInstance.brokerAddrTable列表，向每个Broker地址发送请求码为HEART_BEAT的心跳消息，但是当存在Consumer时才向所有Broker发送心跳消息，否则若不存在Consumer则只向主用Broker地址发送心跳消息；
     */
    private void sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }

        /**
         *
         */
        if (!this.brokerAddrTable.isEmpty()) {
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, HashMap<Long, String>> entry = it.next();
                String brokerName = entry.getKey();
                HashMap<Long, String> oneTable = entry.getValue();
                if (oneTable != null) {
                    for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                        Long id = entry1.getKey();
                        String addr = entry1.getValue();
                        if (addr != null) {
                            if (consumerEmpty) {
                                if (id != MixAll.MASTER_ID)
                                    continue;
                            }

                            try {
                                int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                }
                                this.brokerVersionTable.get(brokerName).put(addr, version);
                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                if (this.isBrokerInNameServer(addr)) {
                                    log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr);
                                } else {
                                    log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                            id, addr);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
                                                      DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                                1000 * 3);
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        //获取路由的数据
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }
                    if (topicRouteData != null) {
                        //从缓存中获取到老的路由等的信息
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //比较一下是否发生了改变
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            //如果未发生改变就得运行下面的参数，判断生产者和消费者是否可用，如果不可用，也算修改了，也就是changed=true
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {
                            //不使用原来的信息，复制数据
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                //设置broker的名字与地址信息
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                //更新TopicPublishInfo中的数据
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                //设置有topic的路由的信息
                                publishInfo.setHaveTopicRouterInfo(true);
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //更新product的路由信息等
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //更新consumer的路由信息等
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        // Consumer
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
                                                    final String topic,
                                                    final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                    fullClassName,
                    RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
                && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                                5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
        }
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 将DefaultMQPushConsumerImpl对象在MQClientInstance中注册，以consumerGroup为key值、DefaultMQPushConsumerImpl
     * 对象为values值存入
     * MQClientInstance.consumerTable:ConcurrentHashMap<String/* group *, MQConsumerInner>变量中，
     * 若在该变量中已存在该consumerGroup的记录则向应用层抛出MQClientException异常；说明在一个客户端的一个
     * 进程下面启动多个Consumer时consumerGroup名字不能一样，否则无法启动；
     * @param group
     * @param consumer
     * @return
     */
    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 给client起个名字，放入table中
     *
     * @param group
     * @param producer
     * @return
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }
        //一个productGroup只能有一个producer进行对应？，那何必叫做组呢？
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            FOR_SEG:
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                        break FOR_SEG;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    /**
     * 找到broker,并且参数中可以指明是否只要这个brokerid,如果不需要，可以从map中找到另外一个给出来
     *
     * @param brokerName
     * @param brokerId
     * @param onlyThisBroker
     * @return
     */
    public FindBrokerResult findBrokerAddressInSubscribe(
            final String brokerName,
            final long brokerId,
            final boolean onlyThisBroker
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        return 0;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
                                                               final String consumerGroup,
                                                               final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }
}
