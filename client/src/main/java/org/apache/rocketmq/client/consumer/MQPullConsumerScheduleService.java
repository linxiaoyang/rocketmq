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
package org.apache.rocketmq.client.consumer;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;

/**
 * Schedule service for pull consumer
 * <p>
 * <p>
 * <p>
 * 与PUSH模式相比，PULL模式需要应用层不间断地进行拉取消息然后再执行消费处理，提高了应用层的编码复杂度，为了Pull方式的编程复杂度，RocketMQ提供了调度消费服务（MQPullConsumerScheduleService），在topic的订阅发送变化（初次订阅或距上次拉取消息超时）就触发PULL方式拉取消息。
 * <p>
 * 1 应用层使用方式
 * <p>
 * 该类是PULL模式下面的调度服务，当RebalanceImpl.processQueueTable队列有变化时才进行消息的拉取，从而降低Pull方式的编程复杂度。在应用层按照如下方式使用：
 * <p>
 * 1、初始化MQPullConsumerScheduleService类，
 * <p>
 * 2、自定义PullTaskCallback接口的实现类，实现该接口的doPullTask(final MessageQueue mq, final PullTaskContext context)方法，在该方法中可以先调用DefaultMQPullConsumer.fetchConsumeOffset (MessageQueue mq, boolean fromStore)方法获取MessageQueue队列的消费进度；然后调用DefaultMQPullConsumer.pull(MessageQueue mq, String subExpression, long offset, int maxNums)方法，该方法是在指定的队列和指定的开始位置读取消息内容；再然后对获取到的消息进行相关的业务逻辑处理；最后可以调用DefaultMQPullConsumer.updateConsumeOffset(MessageQueue mq, long offset)方法进行消费进度的更新，其中offset值是在获取消息内容时返回的下一个消费进度值；
 * <p>
 * 3、调用MQPullConsumerScheduleService.registerPullTaskCallback (String topic, PullTaskCallback callback)方法，在该方法中以topic为key值将自定义的PullTaskCallback 对象存入MQPullConsumerScheduleService. callbackTable:ConcurrentHashMap<String /*topic,PullTaskCallback>变量中；
 * <p>
 * 4、调用MQPullConsumerScheduleService.start()方法启动该调度服务，在启动过程中，首先初始化队列监听器MessageQueueListenerImpl类，该类是MQPullConsumerScheduleService的内部类，实现了MessageQueueListener接口的messageQueueChanged方法；然后将该监听器类赋值给DefaultMQPullConsumer.messageQueueListener变量值；最后调用DefaultMQPullConsumer的start方法启动Consumer；
 * <p>
 * 2 触发拉取消息
 * <p>
 * 在RebalanceImpl.rebalanceByTopic()方法执行的过程中，若RebalanceImpl.processQueueTable有变化，则回调DefaultMQPullConsumer. messageQueueListener变量值的MessageQueueListenerImpl. MessageQueueChanged方法，在该方法中调用MQPullConsumerScheduleService. putTask(String topic, Set<MessageQueue> mqNewSet)方法，其中若为广播模式（BROADCASTING），则mqNewSet为该topic下面的所有MessageQueue队列；若为集群模式，则mqNewSet为给该topic分配的MessageQueue队列，putTask方法的大致逻辑如下：
 * <p>
 * 1、遍历MQPullConsumerScheduleService.taskTable: ConcurrentHashMap<MessageQueue, PullTaskImpl>列表（表示正在拉取消息的任务列表），检查该topic下面的所有MessageQueue对象，若该对象不在入参mqNewSet集合中的，将对应的PullTaskImpl对象的cancelled变量标记为true；
 * <p>
 * 2、对于mqNewSet集合中的MessageQueue对象，若不在MQPullConsumerScheduleService.taskTable列表中，则以MessageQueue对象为参数初始化PullTaskImpl对象（该对象为Runnable线程），然后放入taskTable列表中，并将该PullTaskImpl对象放入MQPullConsumerScheduleService.scheduledThreadPoolExecutor线程池中，然后立即执行该线程。
 * <p>
 * 3 拉取消息的线程（PullTaskImpl）
 * <p>
 * 该PullTaskImpl线程的run方法如下：
 * <p>
 * 1、检查cancelled变量是为true，若为false则直接退出该线程；否则继续下面的处理；
 * <p>
 * 2、以MessageQueue对象的topic值从MQPullConsumerScheduleService.callbackTable变量中获取PullTaskCallback的实现类（该类是由应用层实现）；
 * <p>
 * 3、调用该PullTaskCallback实现类的doPullTask方法，即实现业务层定义的业务逻辑（通用逻辑是先获取消息内容，然后进行相应的业务处理，最后更新消费进度）；
 * <p>
 * 4、再次检查cancelled变量是为true，若不为true，则将该PullTaskImpl对象再次放入MQPullConsumerScheduleService. scheduledThreadPoolExecutor线程池中，设定在200毫秒之后重新调度执行PullTaskImpl线程类；
 */
public class MQPullConsumerScheduleService {
    private final Logger log = ClientLogger.getLog();
    private final MessageQueueListener messageQueueListener = new MessageQueueListenerImpl();
    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
            new ConcurrentHashMap<MessageQueue, PullTaskImpl>();
    private DefaultMQPullConsumer defaultMQPullConsumer;
    private int pullThreadNums = 20;
    private ConcurrentMap<String /* topic */, PullTaskCallback> callbackTable =
            new ConcurrentHashMap<String, PullTaskCallback>();
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public MQPullConsumerScheduleService(final String consumerGroup) {
        this.defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
        this.defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
    }

    public void putTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }

        for (MessageQueue mq : mqNewSet) {
            if (!this.taskTable.containsKey(mq)) {
                PullTaskImpl command = new PullTaskImpl(mq);
                this.taskTable.put(mq, command);
                this.scheduledThreadPoolExecutor.schedule(command, 0, TimeUnit.MILLISECONDS);

            }
        }
    }

    public void start() throws MQClientException {
        final String group = this.defaultMQPullConsumer.getConsumerGroup();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
                this.pullThreadNums,
                new ThreadFactoryImpl("PullMsgThread-" + group)
        );

        this.defaultMQPullConsumer.setMessageQueueListener(this.messageQueueListener);

        this.defaultMQPullConsumer.start();

        log.info("MQPullConsumerScheduleService start OK, {} {}",
                this.defaultMQPullConsumer.getConsumerGroup(), this.callbackTable);
    }

    public void registerPullTaskCallback(final String topic, final PullTaskCallback callback) {
        this.callbackTable.put(topic, callback);
        this.defaultMQPullConsumer.registerMessageQueueListener(topic, null);
    }

    public void shutdown() {
        if (this.scheduledThreadPoolExecutor != null) {
            this.scheduledThreadPoolExecutor.shutdown();
        }

        if (this.defaultMQPullConsumer != null) {
            this.defaultMQPullConsumer.shutdown();
        }
    }

    public ConcurrentMap<String, PullTaskCallback> getCallbackTable() {
        return callbackTable;
    }

    public void setCallbackTable(ConcurrentHashMap<String, PullTaskCallback> callbackTable) {
        this.callbackTable = callbackTable;
    }

    public int getPullThreadNums() {
        return pullThreadNums;
    }

    public void setPullThreadNums(int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    public void setDefaultMQPullConsumer(DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    public MessageModel getMessageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
    }

    public void setMessageModel(MessageModel messageModel) {
        this.defaultMQPullConsumer.setMessageModel(messageModel);
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel =
                    MQPullConsumerScheduleService.this.defaultMQPullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    MQPullConsumerScheduleService.this.putTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    MQPullConsumerScheduleService.this.putTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }


    /**
     * 拉取消息的线程
     */
    class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }


        /**
         * 1、检查cancelled变量是为true，若为false则直接退出该线程；否则继续下面的处理；
         * <p>
         * 2、以MessageQueue对象的topic值从MQPullConsumerScheduleService.callbackTable变量中获取PullTaskCallback的实现类（该类是由应用层实现）；
         * <p>
         * 3、调用该PullTaskCallback实现类的doPullTask方法，即实现业务层定义的业务逻辑（通用逻辑是先获取消息内容，然后进行相应的业务处理，最后更新消费进度）；
         * <p>
         * 4、再次检查cancelled变量是为true，若不为true，则将该PullTaskImpl对象再次放入MQPullConsumerScheduleService. scheduledThreadPoolExecutor线程池中，设定在200毫秒之后重新调度执行PullTaskImpl线程类；
         */
        @Override
        public void run() {
            String topic = this.messageQueue.getTopic();
            if (!this.isCancelled()) {
                PullTaskCallback pullTaskCallback =
                        MQPullConsumerScheduleService.this.callbackTable.get(topic);
                if (pullTaskCallback != null) {
                    final PullTaskContext context = new PullTaskContext();
                    context.setPullConsumer(MQPullConsumerScheduleService.this.defaultMQPullConsumer);
                    try {
                        pullTaskCallback.doPullTask(this.messageQueue, context);
                    } catch (Throwable e) {
                        context.setPullNextDelayTimeMillis(1000);
                        log.error("doPullTask Exception", e);
                    }

                    if (!this.isCancelled()) {
                        MQPullConsumerScheduleService.this.scheduledThreadPoolExecutor.schedule(this,
                                context.getPullNextDelayTimeMillis(), TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                    }
                } else {
                    log.warn("Pull Task Callback not exist , {}", topic);
                }
            } else {
                log.warn("The Pull Task is cancelled, {}", messageQueue);
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }
}
