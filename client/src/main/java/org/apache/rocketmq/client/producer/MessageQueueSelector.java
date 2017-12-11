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
package org.apache.rocketmq.client.producer;

import java.util.List;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;


/**
 * 顺序消息的关键
 * <p>
 * <p>
 * <p>
 * 顺序消息主要是指局部顺序，即生产者通过将某一类消息发送至同一个队列来实现。与发生普通消息相比，
 * 在发送顺序消息时要对同一类型的消息选择同一个队列，即同一个MessageQueue对象。
 * 目前RocketMQ定义了选择MessageQueue对象的接口MessageQueueSelector，
 * 里面有方法select(final List mqs, final Message msg, final Object arg)，
 * 并且RocketMQ默认实现了提供了两个实现类SelectMessageQueueByHash和SelectMessageQueueByRandoom，
 * 即根据arg参数通过Hash或者随机方式选择MessageQueue对象。
 * 为了业务层根据业务需要能自定义选择规则，也可以在业务层自定义选择规则，然后调用
 * DefaultMQProducer.send(Message msg, MessageQueueSelector selector, Object arg)方法完成顺序消息的方式。
 * 与普通消息的发送方法DefaultMQProducer.send(Message msg)相比，在仅仅在选择MessageQueue对象上面有区别
 * ，DefaultMQProducer.send(Message msg, MessageQueueSelector selector, Object arg)方法最终调用DefaultMQProducerImpl.sendSelectImpl(Message msg, MessageQueueSelector selector, Object arg, CommunicationMode communicationMode, SendCallback sendCallback, long timeout)方法，其中 communicationMode等于SYNC，timeout等于DefaultMQProducer.sendMsgTimeout，默认为3秒（表示发送的超时时间为3秒），大致逻辑如下：
 * 1、检查DefaultMQProducerImpl的ServiceState是否为RUNNING，若不是RUNNING状态则直接抛出MQClientException异常给调用者；
 * 2、校验Message消息对象的各个字段的合法性，其中Message对象的body的长度不能大于128KB；
 * 3、以Message消息中的topic为参数调用DefaultMQProducerImpl.tryToFindTopicPublishInfo(String topic)方法从topicPublishInfoTable变量中获取TopicPublishInfo对象；
 * 4、若上一步获取的TopicPublishInfo对象不为空，并且该对象的List队列也不为空，则执行下面的消息发送逻辑，否则抛出MQClientException异常；
 * 5、调用请求参数MessageQueueSelector对象的select(final List mqs, final Message msg, final Object arg)方法选择MessageQueue对象，其中arg参数为send方法中的arg参数，mqs等于TopicPublishInfo对象的List队列；根据此参数来HASH或随机或者自定义规则的方式从List列表中选择MessageQueue对象；
 * 6、若MessageQueue对象不为null，则调用sendKernelImpl(Message msg, MessageQueue mq, CommunicationMode communicationMode, SendCallback sendCallback, long timeout)进行消息的发送工作；
 */
public interface MessageQueueSelector {
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
