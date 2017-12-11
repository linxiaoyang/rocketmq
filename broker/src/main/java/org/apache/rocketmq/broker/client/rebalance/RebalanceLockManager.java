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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于group，mq，clientId实现了一个对象锁
 */
public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
            "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                                "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                        return true;
                    }

                    log.warn(
                            "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 判断clientId是否是锁住的，如果已经被锁住了，就更新最后更新的时间
     *
     * @param group
     * @param mq
     * @param clientId
     * @return
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }


    /**
     *
     * 客户端顺序消费时锁住MessageQueue队列的请求
     *
     *
     * RebalanceLockManager.tryLockBatch(String group, Set<MessageQueue> mqs, String clientId)方法，
     * 该方法批量锁MessageQueue队列，返回锁定成功的MessageQueue队列集合，该方法的入参group为ConsumerGroup、
     * mqs为该brokerName对应的MessageQueue集合（Consumer端传来的），clientId为Consumer端的ClientId，大致逻辑如下
     * <p>
     * 在Broker端有数据结构RebalanceLockManager.mqLockTable:ConcurrentHashMap<String/* group , ConcurrentHashMap<MessageQueue, LockEntry>>，表示一个consumerGroup下面的所有MessageQueue的锁的情况，锁是由LockEntry类标记的，在LockEntry类中clientId和lastUpdateTimestamp两个变量，表示某个客户端（clientId）在某时间（lastUpdateTimestamp）对MessageQueue进行锁住，锁的超时时间为60秒；
     * <p>
     * 1、初始化临时变量lockedMqs:Set<MessageQueue>和notLockedMqs: Set<MessageQueue>,分别存储锁住的MessageQueue集合和未锁住的MessageQueue集合；
     * <p>
     * 2、遍历请求参数MessageQueue集合，以请求参数group、每个MessageQueue对象、clientId为参数检查该MessageQueue对象是否被锁住，逻辑是：以group从RebalanceLockManager.mqLockTable中获取该consumerGroup下面的所有ConcurrentHashMap<MessageQueue, LockEntry>集合：若该集合不为空再以MessageQueue对象获取对应的LockEntry标记类，检查该LockEntry类是否被该clientId锁住以及锁是否过期，若是被该clientId锁住且没有过期，则更新LockEntry标记类的lastUpdateTimestamp变量；若该集合为空则认为没有锁住。若遍历的MessageQueue对象被该ClientId锁住且锁未超期了则将MessageQueue对象存入lockedMqs变量中，否则将MessageQueue对象存入notLockedMqs变量中。
     * <p>
     * 3、若notLockedMqs集合不为空，即表示有未锁住的MessageQueue队列，继续下面的处理；下面的逻辑处理块是加锁的，就是说同时只有一个线程执行该逻辑块。
     * <p>
     * 4、以group从RebalanceLockManager.mqLockTable中获取该consumerGroup下面的所有ConcurrentHashMap<MessageQueue, LockEntry>集合取名groupvalue集合，若该集合为空，则新建一个ConcurrentHashMap<MessageQueue,LockEntry>集合并赋值给groupvalue集合，然后以consumerGroup为key值存入RebalanceLockManager.mqLockTable变量中；
     * <p>
     * 5、遍历notLockedMqs集合，以每个MessageQueue对象从groupvalue集合中取LockEntry标记类，若该类为null（在第4步新建的集合会出现此情况）则初始化LockEntry类，并设置该类的clientId为该Consumer的clientId并存入groupvalue集合中；若该类不为null，则再次检查是否被该clientId锁住，若是则更新LockEntry标记类的lastUpdateTimestamp变量，并添加到lockedMqs集合中，然后继续遍历notLockedMqs集合中的下一个MessageQueue对象；若仍然未被锁则检查锁是否超时，若已经超时则设置该LockEntry类的clientId为该Consumer的clientId并更新lastUpdateTimestamp变量；
     * <p>
     * 6、返回lockedMqs集合给Consumer端；
     *
     * @param group
     * @param mqs
     * @param clientId
     * @return
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
                                          final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                    "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                    group,
                                    clientId,
                                    mq);
                        }

                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                    "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                    group,
                                    oldClientId,
                                    clientId,
                                    mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                                "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }


    /**
     * 客户端顺序消费时解锁MessageQueue队列的请求
     * <p>
     * 在Broker端收到UNLOCK_BATCH_MQ请求码之后，间接地调用了RebalanceLockManager.unlockBatch(String consumerGroup, Set<MessageQueue> mqs, String clientId)方法，该方法批量解锁请求中的MessageQueue队列；
     * <p>
     * 1、以consumerGroup为key值从RebalanceLockManager.mqLockTable:ConcurrentHashMap<String/* group , ConcurrentHashMap<MessageQueue, LockEntry>>变量中获取对应的ConcurrentHashMap<MessageQueue, LockEntry>集合；
     * <p>
     * 2、遍历入参Set<MessageQueue>集合，以该集合中的每个MessageQueue对象从上一步获得的ConcurrentHashMap<MessageQueue, LockEntry>集合中获取LockEntry标记类，若该类不为null，则检查该LockEntry类的ClientId是否等于请求中的ClientId（请求端的ClientId）若相等则将该MessageQueue对象的记录从ConcurrentHashMap<MessageQueue, LockEntry>集合中移除；其他情况均不处理；
     *
     * @param group
     * @param mqs
     * @param clientId
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                        group,
                                        mq,
                                        clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                        lockEntry.getClientId(),
                                        group,
                                        mq,
                                        clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                            group,
                            clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            boolean expired =
                    (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
