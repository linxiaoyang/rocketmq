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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }


    /**
     * 5.1)Producer的链接。检查ProducerManager.groupChannelTable：HashMap<String, HashMap<Channel, ClientChannelInfo>>变量，查看每个ClientChannelInfo的lastUpdateTimestamp距离现在是否已经超过了120秒，若是则从该变量中删除此链接。
     * <p>
     * 5.2)Consumer的链接。检查ConsumerManager.consumerTable: ConcurrentHashMap<String, ConsumerGroupInfo>变量，查看每个ClientChannelInfo的lastUpdateTimestamp距离现在是否已经超过了120秒，若是则从该变量中删除此链接。
     * <p>
     * 5.3)过滤服务器的链接。检查FilterServerManager.filterServerTable：ConcurrentHashMap<Channel, FilterServerInfo>，同样是检查lastUpdateTimestamp变量值距离现在是否已经超过了120秒，若是则从该变量中删除此链接。
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 在启动Broker的过程中启动该线程服务，主要是对于未拉取到的信息进行补偿，在后台每个1秒钟进行尝试拉取信息。该线程run方法的大致逻辑如下：
     * <p>
     * 遍历PullRequestHoldService.pullRequestTable: ConcurrentHashMap<String/* topic@queueid ,ManyPullRequest>变量中的每个Key值,其中ManyPullRequest对象是一个封装ArrayList<PullRequest>列表的对象；由Key值解析出topic和queueId，然后调用DefaultMessageStore.getMaxOffsetInQuque(String topic,int queueId)方法获取该topic和queueId下面的队列的最大逻辑Offset，再调用PullRequestHoldService.notifyMessageArriving(String topic, int queueId, long maxOffset)方法。主要的拉取消息逻辑在此方法中。
     * <p>
     * 1、根据topic和queueId构建key值从pullRequestTable中获取ManyPullRequest对象，该对象中包含了PullRequest的集合列表；
     * <p>
     * 2、遍历该PullRequest的集合中的每个PullRequest对象，遍历步骤如下：
     * <p>
     * 2.1）检查当前最大逻辑offset（入参maxOffset）是否大于该PullRequest对象的pullFromThisOffset值（该值等于当初请求消息的queueOffset值）；
     * <p>
     * 2.2）若当前最大逻辑offset大于请求的queueOffset值，说明请求的读取偏移量已经有数据达到了，则调用PullMessageProcessor.excuteRequestWhenWakeup(Channel channel, RemotingCommand request)方法进行消息的拉取，在该方法中的处理逻辑与收到PULL_MESSAGE请求码之后的处理逻辑基本一致，首先创建一个线程，然后将该线程放入线程池中，该线程的主要目的是调用PullMessageProcessor.processRequest(Channel channel, RemotingCommand request, boolean brokerAllowSuspend)方法，其中入参brokerAllowSuspend等于false，表示若未拉取到消息，则不再采用该线程的补偿机制了；然后继续遍历下一个PullRequest对象；
     * <p>
     * 2.3）若当前最大逻辑offset小于请求的queueOffset值；则再次读取最大逻辑offset进行第2.2步的尝试，若当前最大逻辑offset还是小于请求的queueOffset值，则检查该请求是否超时，即PullRequest对象的suspendTimestamp值加上timeoutMillis值是否大于当前时间戳，若已超时则直接调用PullMessageProcessor.excuteRequestWhenWakeup(Channel channel, RemotingCommand request)方法进行消息的拉取，然后继续遍历下一个PullRequest对象；若未超时则将该PullRequest对象存入临时的PullRequest列表中，该列表中的PullRequest对象是数据未到达但是也未超时的请求，然后继续遍历下一个PullRequest对象直到PullRequest集合遍历完为止；
     * <p>
     * 2.4）将临时的PullRequest列表重新放入PullRequestHoldService.pullRequestTable变量中，等待下一次的遍历。
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
                                      long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
