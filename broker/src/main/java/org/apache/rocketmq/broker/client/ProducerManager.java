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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private final Lock groupChannelLock = new ReentrantLock();
    /**
     * product group 与 channel 之间的映射关系
     */
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

    public ProducerManager() {
    }

    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
                new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    newGroupChannelTable.putAll(groupChannelTable);
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    /**
     * 那些超时的channel,要关闭的
     */
    public void scanNotActiveChannel() {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                it.remove();
                                log.warn(
                                        "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                        RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                                .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                    entry.getValue();
                            final ClientChannelInfo clientChannelInfo =
                                    clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                log.info(
                                        "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                        clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 注册Producer信息
     * <p>
     * <p>
     * 调用ProducerManager.registerProducer(String groupName,
     * <p>
     * ClientChannelInfo clientChannelInfo)方法进行Producer注册。就是更新ProducerManager.groupChannelTable:HashMap<String,HashMap<Channel,ClientChannelInfo>>变量值。大致逻辑如下：
     * <p>
     * 1、以groupName从ProducerManager.groupChannelTable: HashMap<String,HashMap<Channel,ClientChannelInfo>>变量中获取values值；
     * <p>
     * 2、若没有获取到，则创建一个新的HashMap<Channel,ClientChannelInfo>作为values值，并以groupName值为key值存入groupChannelTable遍历中；
     * <p>
     * 3、以Chanel为key值从上面的values值中获取ClientChannelInfo对象；若该对象为空，则将该Channel和请求参数clientChannelInfo添加到此values值中。
     * <p>
     * 4、更新更新ClientChannelInfo对象的时间戳；
     * <p>
     * 以相同的逻辑将Producer的clientChannelInfo对象存入hashcodeChannelTable:HashMap<Integer /* group hash code , List<ClientChannelInfo>>变量中，只是key值为groupName的hash值；
     *
     * @param group
     * @param clientChannelInfo
     */
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                                clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                    clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }
}
