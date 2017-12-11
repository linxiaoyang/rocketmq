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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    /**
     * 组名与消费组（ConsumerGroupInfo）的映射消息
     */
    private final ConcurrentMap<String/* Group */, ConsumerGroupInfo> consumerTable =
            new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    /**
     * 根据group与clientId找到指定的ChannelInfo
     *
     * @param group
     * @param clientId
     * @return
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    /**
     * 根据topic和group查找Consumer订阅信息
     * <p>
     * <p>
     * 调用ConsumerManager.findSubscriptionData(String group, String topic)方法查询当前的订阅信息。
     * <p>
     * 1、从consumerTable:ConcurrentHashMap<String/* Group , ConsumerGroupInfo>变量以group获取相应的 ConsumerGroupInfo对象；若该对象为空，则返回null；
     * <p>
     * 2、若该对象不为空，则从该ConsumerGroupInfo对象的subscriptionTable:ConcurrentHashMap<String/* Topic , SubscriptionData>变量中获取SubscriptionData对象；并返回SubscriptionData对象，即为订阅信息。
     *
     * @param group
     * @param topic
     * @return
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }

    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                                next.getKey());
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }

                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
    }

    /**
     * consumer调用，注册consumer
     * <p>
     * <p>
     * <p>
     * 调用ConsumerManager.registerConsumer(String groupname, ClientChannelInfo clientChannelInfo, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, Set<SubscriptionData> subList)方法进行Consumer的注册。就是更新ConsumerManager.consumerTable:ConcurrentHashMap<String/* Group , ConsumerGroupInfo>变量的值，大致逻辑如下：
     * <p>
     * 1、以参数groupname从ConsumerManager.consumerTable中获取ConsumerGroupInfo对象；若没有，则初始化ConsumerGroupInfo对象并存入consumerTable列表中，并返回该ConsumerGroupInfo对象；
     * <p>
     * 2、更新该ConsumerGroupInfo对象中的对应的渠道对象ClientChannelInfo对象的信息并返回update值，初始化为false。以该链接的Channel从ConsumerGroupInfo.channelInfoTable:ConcurrentHashMap<Channel, ClientChannelInfo>变量中获取ClientChannelInfo对象，若该对象为空，则将请求参数中的ClientChannelInfo对象存入该Map变量中，并且认为Channel被更新过故置update=true；若该对象不为空则检查已有的ClientChannelInfo对象的ClientId值是否与新传入的ClientChannelInfo对象的ClientId值一致，若不一致，则替换该渠道信息；最后更新ClientChannelInfo的时间戳；
     * <p>
     * 3、更新该ConsumerGroupInfo对象中的订阅信息。遍历请求参数Set<SubscriptionData>集合中的每个SubscriptionData对象，初始化置update=false；
     * <p>
     * 3.1）以SubscriptionData对象的topic值从ConsumerGroupInfo.subscriptionTable变量中获取已有的SubscriptionData对象；若获取的SubscriptionData对象为null，则以topic值为key值将遍历到的该SubscriptionData对象存入subscriptionTable变量中，并且认为订阅被更新过故置update=true；否则若已有的SubscriptionData对象的SubVersion标记小于新的SubscriptionData对象的SubVersion标记，就更新subscriptionTable变量中已有的SubscriptionData对象；
     * <p>
     * 3.2）检查ConsumerGroupInfo.subscriptionTable变量中每个topic，若topic不等于请求参数SubscriptionData集合的每个SubscriptionData对象的topic变量值；则从subscriptionTable集合中将该topic的记录删除掉，并且认为订阅被更新过故置update=true；
     * <p>
     * 3.4）更新ConsumerGroupInfo对象中的时间戳；
     * <p>
     * 4、若上述两步返回的update变量值有一个为true，则调用DefaultConsumerIdsChangeListener.consumerIdsChanged(String group, List<Channel> channels)方法，向每个Channel下的Consumer发送NOTIFY_CONSUMER_IDS_CHANGED请求码，在Consumer收到请求之后调用线程的wakeup方法，唤醒RebalanceService服务线程；
     *
     * @param group
     * @param clientChannelInfo
     * @param consumeType
     * @param messageModel
     * @param consumeFromWhere
     * @param subList
     * @param isNotifyConsumerIdsChangedEnable
     * @return
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
                                    ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
                                    final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {

        /**
         * 从缓存中根据消费组的组名查询消费组
         */
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            //不存在就创建
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            //塞到缓存中
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        //更新channel
        boolean r1 =
                consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                        consumeFromWhere);
        //更新订阅信息
        boolean r2 = consumerGroupInfo.updateSubscription(subList);
        //上面两个操作有一个成功了的话
        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable) {
                //走改变的逻辑
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
        //走注册的请求
        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }

    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
                                   boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            //从本地缓存中去除这个client配置信息
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                //从consumerTable删除这个group
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    log.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);

                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            if (isNotifyConsumerIdsChangedEnable) {
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                    consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                //超时了
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                            "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                            RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    //关掉这个链接
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    //删掉这个channel的配置
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                        group);
                it.remove();
            }
        }
    }

    /**
     * 查找有这个topic的group
     *
     * @param topic
     * @return
     */
    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                    entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }
}
