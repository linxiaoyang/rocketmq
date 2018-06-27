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
package org.apache.rocketmq.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filtersrv.FilterServerManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.MessageStoreFactory;
import org.apache.rocketmq.broker.plugin.MessageStorePluginContext;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_PROTECTION = LoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
    private static final Logger LOG_WATER_MARK = LoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ConsumerOffsetManager consumerOffsetManager;
    private final ConsumerManager consumerManager;
    private final ConsumerFilterManager consumerFilterManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final Broker2Client broker2Client;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    private final SlaveSynchronize slaveSynchronize;
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    private final FilterServerManager filterServerManager;
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private MessageStore messageStore;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;
    private TopicConfigManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService consumerManageExecutor;
    private boolean updateMasterHAServerAddrPeriodically = false;
    private BrokerStats brokerStats;
    private InetSocketAddress storeHost;
    private BrokerFastFailure brokerFastFailure;
    private Configuration configuration;

    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig,
            final MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.consumerFilterManager = new ConsumerFilterManager(this);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));

        this.brokerFastFailure = new BrokerFastFailure(this);
        this.configuration = new Configuration(
                log,
                BrokerPathConfigHelper.getBrokerConfigPath(),
                this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }


    /**
     * Broker的初始化过程
     * <p>
     * <p>
     * <p>
     * <p>
     * 调用BrokerController对象的initialize方法进行初始化工作。大致逻辑如下：
     * <p>
     * 1、加载topics.json、consumerOffset.json、subscriptionGroup.json文件，
     * 分别将各文件的数据存入TopicConfigManager、ConsumerOffsetManager、SubscriptionGroupManager对象中；
     * 在初始化BrokerController对象的过程中，初始化TopicConfigManager对象时，
     * 默认初始化了"SELF_TEST_TOPIC"、"TBW102"（自动创建Topic功能是否开启
     * BrokerConfig.autoCreateTopicEnable=false，线上建议关闭）、
     * "BenchmarkTest"、BrokerClusterName（集群名称）、BrokerName（Broker的名称）、
     * "OFFSET_MOVED_EVENT"这5个topic的信息并存入了topicConfigTable变量中，
     * 在向NameServer注册时发给NameServer进行登记；
     * <p>
     * 2、初始化DefaultMessageStore对象，该对象是应用层访问存储层的访问类；
     * <p>
     * 2.1)AllocateMapedFileService服务线程，当需要创建MappedFile时
     * （在MapedFileQueue.getLastMapedFile方法中），向该线程的requestQueue队列中放入AllocateRequest请求对象，
     * 该线程会在后台监听该队列，并在后台创建MapedFile对象，即同时创建了物理文件。
     * <p>
     * 2.2）创建DispatchMessageService服务线程，该服务线程负责给commitlog数据创建ConsumeQueue数据和
     * 创建Index索引。
     * <p>
     * 2.3）创建IndexService服务线程，该服务线程负责创建Index索引；
     * <p>
     * 2.4）还初始化了如下服务线程，在调用DispatchMessageService对象的start方法时启动这些线程：
     * <p>
     * 服务类名	作用
     * FlushConsumeQueueService	逻辑队列刷盘服务，每隔1秒钟就将ConsumeQueue逻辑队列、
     * TransactionStateService.TranRedoLog变量的数据持久化到磁盘物理文件中
     * CleanCommitLogService	清理物理文件服务，定期清理72小时之前的物理文件。
     * CleanConsumeQueueService	清理逻辑文件服务，定期清理在逻辑队列中的物理偏移量小于
     * commitlog中的最小物理偏移量的数据，同时也清理Index中物理偏移量小于commitlog中的最小物理偏移量的数据。
     * StoreStatsService	存储层内部统计服务
     * HAService	用于commitlog数据的主备同步
     * ScheduleMessageService	用于监控延迟消息，并到期后执行
     * TransactionStateService	用于事务消息状态文件，在RocketMQ-3.1.9版本中可以查看源码
     * 2.5）初始化CommitLog对象，在初始化该对象的过程中，第一，根据刷盘类型初始化
     * FlushCommitLogService线程服务，若为同步刷盘（SYNC_FLUSH），则创建初始化为
     * GroupCommitService线程服务；若为异步刷盘（ASYNC_FLUSH）则创建初始化FlushRealTimeService线程服务；
     * 第二，初始化DefaultAppendMessageCallback对象；
     * <p>
     * 2.6）启动AllocateMapedFileService、DispatchMessageService、IndexService服务线程。
     * <p>
     * 3、调用DefaultMessageStore.load加载数据：
     * <p>
     * 3.1)调用ScheduleMessageService.load方法，初始化延迟级别列表。
     * 将这些级别（"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"）
     * 的延时存入延迟级别delayLevelTable：ConcurrentHashMap<Integer /* level , Long/* delay timeMillis >变量中，
     * 例如1s的kv值为1:1000,5s的kv值为2:5000，key值依次类推；每个延迟级别即为一个队列。
     * <p>
     * 3.2)调用CommitLog.load方法，在此方法中调用MapedFileQueue.load方法，
     * 将$HOME /store/commitlog目录下的所有文件加载到MapedFileQueue的List<MapedFile>变量中；
     * <p>
     * 3.3)调用DefaultMessageStore.loadConsumeQueue方法加载consumequeue文件数据到
     * DefaultMessageStore.consumeQueueTable集合中。
     * <p>
     * 3.4）调用TransactionStateService.load()方法加载tranStateTable文件
     * 和tranRedoLog文件；
     * <p>
     * 3.5)初始化StoreCheckPoint对象，加载$HOME/store/checkpoint文件，
     * 该文件记录三个字段值，分别是物理队列消息时间戳、逻辑队列消息时间戳、索引队列消息时间戳。
     * <p>
     * 3.6)调用IndexService.load方法加载$HOME/store/index目录下的文件。
     * 对该目录下的每个文件初始化一个IndexFile对象。然后调用IndexFile对象的load方法
     * 将IndexHeader加载到对象的变量中；再根据检查是否存在abort文件，若有存在abort文件，
     * 则表示Broker表示上次是异常退出的，则检查checkpoint的indexMsgTimestamp字段值是
     * 否小于IndexHeader的endTimestamp值，indexMsgTimestamp值表示最后刷盘的时间，
     * 若小于则表示在最后刷盘之后在该文件中还创建了索引，则要删除该Index文件，否则将该
     * IndexFile对象放入indexFileList:ArrayList<IndexFile>索引文件集合中。
     * <p>
     * 3.7)恢复内存数据。
     * <p>
     * 1）恢复每个ConsumeQueue对象的maxPhysicOffset变量的值（最后一个消息的物理offset），
     * 遍历consumeQueueTable集合中的每个topic/queueId下面的ConsumeQueue对象，
     * 调用ConsumeQueue对象recover方法。
     * <p>
     * 2）根据是否有abort文件来确定选择何种方法恢复commitlog数据，若有该文件则调
     * 用CommitLog对象的recoverAbnormally方法进行恢复，否则调用CommitLog对象的
     * recoverNormally方法进行恢复。主要是恢复MapedFileQueue对象的commitedWhere变量值
     * （即刷盘的位置），删除该commitedWhere值所在文件之后的commitlog文件以及对应的MapedFile对象。
     * <p>
     * 3）恢复CommitLog对象的topicQueueTable :HashMap<String/* topic-queueid , Long/* offset >变量值；
     * 遍历consumeQueueTable集合，逻辑如下：
     * <p>
     * A）对每个topic/queueId下面的ConsumeQueue对象，获取该对象的最大偏移量，等于MapedFileQueue.getMaxOffset/20；然后以topic-queueId为key值，该偏移量为values，存入topicQueueTable变量中；
     * <p>
     * B）以commitlog的最小物理偏移量修正ConsumeQueue对象的最小逻辑偏移量minLogicOffset；
     * <p>
     * 4）调用TransactionStateService.tranRedoLog:ConsumeQueue对象的recover()方法恢复tranRedoLog文件；
     * <p>
     * 5）调用TransactionStateService.recoverStateTable(boolean lastExitOK)方法恢复tranStateTable文件，
     * <p>
     * 5.1）若存在abort文件则上一次退出是异常关闭，采用如下恢复方式：
     * <p>
     * A）删除tranStateTable文件；
     * <p>
     * B）新建tranStateTable文件,然后从tranRedoLog文件的开头开始偏移，找出所有事务消息状态为PREPARED的记录，该记录为每个事务消息在commitlog的物理位置；
     * <p>
     * C）遍历这些PREPARED状态的事务消息的物理位置集合，以物理位置commitlogoffset值从commitlog文件中找出获取每个事务消息的commitlogoffset、msgSize、StoreTimeStamp以及producerGroup的hash值，然后调用TransactionStateService.appendPreparedTransaction(long clOffset, int size, int timestamp, int groupHashCode)方法将这些信息写入新建的tranStateTable文件中，并更新TransactionStateService.tranStateTableOffset变量值（表示在tranStateTable文件中的消息个数）；
     * <p>
     * 5.2）若不存在abort文件则上一次是正常关闭，采用如下恢复方式：
     * <p>
     * 与调用ConsumeQueue对象的recover方法恢复ConsumeQueue数据的逻辑是一样的；在恢复完tranStateTable文件数据之后计算该文件中的消息个数并赋值给TransactionStateService.tranStateTableOffset变量值，计算方式为MapedFileQueue.getMaxOffset()/24；
     * <p>
     * 4、初始化Netty服务端NettyRemotingServer对象；
     * <p>
     * 5、初始化发送消息线程池（sendMessageExecutor）、拉取消息线程池（pullMessageExecutor）、管理Broker线程池（adminBrokerExecutor）、客户端管理线程池（clientManageExecutor）。
     * <p>
     * 6、注册事件处理器，包括发送消息事件处理器（SendMessageProcessor）、拉取消息事件处理器、查询消息事件处理器（QueryMessageProcessor，包括客户端的心跳事件、注销事件、获取消费者列表事件、更新更新和查询消费进度consumerOffset）、客户端管理事件处理器、结束事务处理器（EndTransactionProcessor）、默认事件处理器（AdminBrokerProcessor）。
     * <p>
     * 7、启动如下定时任务：
     * <p>
     * 1）Broker的统计功能；
     * <p>
     * 2）每隔5秒对ConsumerOffsetManager.offsetTable: ConcurrentHashMap<String/* topic@group , ConcurrentHashMap<Integer, Long>>变量的内容进行持久化操作，持久化到consumerOffset.json文件中；
     * <p>
     * 3）扫描被删除的topic，将offsetTable中的对应的消费进度记录也删掉。大致逻辑是遍历ConsumerOffsetManager.offsetTable变量：
     * <p>
     * A）以group和topic为参数调用ConsumerManager.findSubscriptionData(String group, String topic)方法获取当前该topic和group的订阅数据；若获取的数据为空，则认为该topic和group已经不在当前订阅关系中。
     * <p>
     * B）遍历该topic@group下面的所有队列的消费进度（ConsumerOffsetManager.offsetTable变量的values值），检查每个队列的消费进度是否都大于该队列最小逻辑偏移量；
     * <p>
     * C）若该topic和group已经不在当前订阅数据表中，而且在offsetTable中的消费进度已经小于ConsumeQueue的最小逻辑偏移量minLogicOffset，则从offsetTable变量中删除该记录。
     * <p>
     * 8、检查Broker配置的Name Server地址是否为空，若不为空，则更新Broker的Netty客户端BrokerOuterAPI.NettyRemotingClient的Name Server地址；若为空且配置了允许从地址服务器找Name Server地址，则启动定时任务，地址服务器的路径是"http://&quot; + WS_DOMAIN_NAME + ":8080/rocketmq/" + WS_DOMAIN_SUBGROUP ；其中WS_DOMAIN_NAME由配置参数rocketmq.namesrv.domain设置，WS_DOMAIN_SUBG由配置参数rocketmq.namesrv.domain.subgroup设置；
     * <p>
     * 9、若该Broker为主用，则启动定时任务打印主用Broker和备用Broker在Commitlog上的写入位置相差多少个字节。
     * <p>
     * 主用Broker的写入位置计算方式：获取最后一个MappedFile的fileFromOffset和wrotePostion值，相加即为最新的写入位置。
     * <p>
     * 备用Broker的写入位置从HAService. push2SlaveMaxOffset获得，该值表示主用Broker同步到Slave的最大Offset。
     * <p>
     * 10、若该Broker为备用，首先检查是否配置了主用Broker的地址，在备用Broker可以在broker.properties配置文件中设置haMasterAddress参数来指定主用Broker的地址，若设置了，则更新HAService.HAClient.masterAddress的值；若没有配置，则设置标志位updateMasterHAServerAddrPeriodically等于true，在该Broker向NameServer注册的时候根据NameServer返回的主用Broker地址来设置HAClient.masterAddress值；说明在配置参数haMasterAddress设置的主用地址的优先级要高于NameServer返回的主用Broker地址。
     * <p>
     * 11、若该Broker为备用，设置同步Config文件的定时任务，每隔60秒调用SlaveSynchronize.syncAll()方法向主用Broker请求一次config类文件的同步。
     *
     * @return
     * @throws CloneNotSupportedException
     */
    public boolean initialize() throws CloneNotSupportedException {
        /**
         * 启动的时候将信息从本地的存储文件加在到内存中
         */
        boolean result = this.topicConfigManager.load();

        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();

        if (result) {
            try {
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                                this.brokerConfig);
                this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                //load plugin
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                this.messageStore = MessageStoreFactory.build(context, this.messageStore);
                this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
            } catch (IOException e) {
                result = false;
                log.error("Failed to initialize", e);
            }
        }

        result = result && this.messageStore.load();

        if (result) {
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getSendMessageThreadPoolNums(),
                    this.brokerConfig.getSendMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.sendThreadPoolQueue,
                    new ThreadFactoryImpl("SendMessageThread_"));

            this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getPullMessageThreadPoolNums(),
                    this.brokerConfig.getPullMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.pullThreadPoolQueue,
                    new ThreadFactoryImpl("PullMessageThread_"));

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                            "AdminBrokerThread_"));

            this.clientManageExecutor = new ThreadPoolExecutor(
                    this.brokerConfig.getClientManageThreadPoolNums(),
                    this.brokerConfig.getClientManageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.clientManagerThreadPoolQueue,
                    new ThreadFactoryImpl("ClientManageThread_"));

            this.consumerManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getConsumerManageThreadPoolNums(), new ThreadFactoryImpl(
                            "ConsumerManageThread_"));

            this.registerProcessor();

            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    } catch (Throwable e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerFilterManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumer filter error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.protectBroker();
                    } catch (Throwable e) {
                        log.error("protectBroker error.", e);
                    }
                }
            }, 3, 3, TimeUnit.MINUTES);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.printWaterMark();
                    } catch (Throwable e) {
                        log.error("printWaterMark error.", e);
                    }
                }
            }, 10, 1, TimeUnit.SECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
                    } catch (Throwable e) {
                        log.error("schedule dispatchBehindBytes error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
                log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        } catch (Throwable e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        } catch (Throwable e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }


    /**
     * 注册处理器
     */
    public void registerProcessor() {
        /**
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

        /**
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    public void protectBroker() {
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            final Iterator<Map.Entry<String, MomentStatsItem>> it = this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, MomentStatsItem> next = it.next();
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0)
            slowTimeMills = 0;

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("Slave fall behind master: {} bytes", diff);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }
    }

    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public void start() throws Exception {
        /**
         * 调用DefaultMessageStore.start方法启动DefaultMessageStore对象中的一些服务线程
         */
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        /**
         * 启动Broker的Netty服务端NettyRemotingServer。监听消费者或生产者发起的请求信息并处理；
         */
        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        /**
         * 启动BrokerOuterAPI中的NettyRemotingClient，即建立与NameServer的链接，用于自身Broker与其他模块的RPC功能调用；
         * 包括获取NameServer的地址、注册Broker、注销Broker、获取Topic配置、获取消息进度信息、获取订阅关系等RPC功能
         */
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        /**
         * 启动拉消息管理服务PullRequestHoldService，当拉取消息时未发现消息，则初始化PullRequeset对象放入该服务线程
         * 的pullRequestTable列表中，由PullRequestHoldService每隔1秒钟就检查一遍每个PullRequeset对象要读取的数据
         * 位置在consumequeue中是否已经有数据了，若有则交由PullManageProcessor处理。
         */
        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        /**
         * 启动ClientHousekeepingService服务，在启动过程中设置定时任务，该定时任务每隔10秒就检查一次客户端的链接情况，
         * 清除不活动的链接（即在120秒以内没有数据交互了）
         */
        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        /**
         * 启动FilterServerManager，每隔30秒定期一次检查Filter Server个数，若没有达到16个则创建；
         */
        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        /**
         * 首先调用BrokerController.registerBrokerAll方法立即向NameServer注册Broker；
         * 然后设置定时任务，每隔30秒调用一次该方法向NameServer注册；
         */
        this.registerBrokerAll(true, false);

        /**
         * 启动每隔5秒对未使用的topic数据进行清理的定时任务。该定时任务调用
         * DefaultMessageStore.cleanUnusedTopic(Set<String>topics)方法
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    /**
     * 调用BrokerController.registerBrokerAll方法立即向NameServer注册Broker
     *
     * @param checkOrderConfig
     * @param oneway
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        /**
         * 将TopicConfigManager.topicConfigTable变量序列化成TopicConfigSerializeWrapper对象；
         */
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                                this.brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        /**
         * 到namesrv上注册所有的broker
         *
         * BrokerOuterAPI.registerBrokerAll(String clusterName, StringbrokerAddr, String brokerName, long brokerId, String haServerAddr, TopicConfigSerializeWrapper topicConfigWrapper, List<String> filterServerList, boolean oneway)方法向Name Server注册；
         * 其中haServerAddr等于"该Broker的本地IP地址:端口号（默认为10912）"；
         * 在该方法中遍历所有的NameServer地址，对于每个NameServer地址均调用BrokerOuterAPI.registerBroker方法，
         * 在registerBroker方法中创建RegisterBrokerRequestHeader对象，然后向NameServer发送REGISTER_BROKER请求码；
         */
        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.getHAServerAddr(),
                topicConfigWrapper,
                this.filterServerManager.buildNewFilterServerList(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMills());

        if (registerBrokerResult != null) {
            /**
             * 根据updateMasterHAServerAddrPeriodically标注位
             * （在初始化时若Broker的配置文件中没有haMasterAddress参数配置，则标记为true，
             * 表示注册之后需要更新主用Broker地址）以及NameServer返回的HaServerAddr地址是否为空，
             * 若标记位是true且返回的HaServerAddr不为空，则用HaServerAddr地址更新HAService.HAClient.masterAddress的值；
             * 该HAClient.masterAddress值用于主备Broker之间的commitlog数据同步之用
             */
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            /**
             * 用NameServer返回的MasterAddr值更新SlaveSynchronize.masterAddr值，用于主备Broker同步Config文件使用；
             */
            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            /**
             * 根据Name Server上的配置参数ORDER_TOPIC_CONFIG的值来更新Broker端的TopicConfigManager.topicConfigTable变量值，对于在ORDER_TOPIC_CONFIG中配置了的topic，将该topic的配置参数order置为true，否则将该topic的配置参数order置为false；
             */
            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
        }
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
