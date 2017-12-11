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

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 这个类是应该想要发送数据的入口点
 * This class is the entry point for applications intending to send messages.
 * </p>
 * <p>
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios.
 * </p>
 * <p>
 * This class aggregates various <code>send</code> methods to deliver messages to brokers. Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding.
 * </p>
 * <p>
 * <p>
 * <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context.
 * </p>
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved.
     * </p>
     * <p>
     * For non-transactional messages, it does not matter as long as it's unique per process.
     * </p>
     * <p>
     * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     */
    private String createTopicKey = MixAll.DEFAULT_TOPIC;

    /**
     * Number of queues to create per default topic.
     * 默认的topic的队列数
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     * 发送消息的超时时间
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     * 数据大于4K就会被压缩，这是个阀值
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
     * </p>
     * <p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     * <p>
     * 在同步模式下，发送失败的重试次数
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode.
     * </p>
     * <p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     * 在异步模式下，发送失败的重试次数
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     * 当一个Broker发送失败，是否需要切换到另外一个Broker
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message size in bytes.
     * 发送消息的最大容量
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Default constructor.
     */
    public DefaultMQProducer() {
        this(MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * Constructor specifying both producer group and RPC hook.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook       RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(producerGroup, null);
    }

    /**
     * Constructor specifying the RPC hook.
     *
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * Start this producer instance.
     * </p>
     * <p>
     * <strong>
     * Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must to invoke
     * this method before sending or querying messages.
     * </strong>
     * </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
    }

    /**
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     * <p>
     * 获取发送消息的队列，但是这个方法都从远程调用数据，然后放到本地
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * <p>
     * 同步模式发送数据，只有发送逻辑处理完成后才会返回
     * <p>
     * <p>
     * <p>
     * 以DefaultMQProducer.send(Message msg)方法为例，讲解Producer消息的发送逻辑，该方法最终调用DefaultMQProducerImpl.sendDefaultImpl(Message msg, CommunicationMode communicationMode, SendCallback sendCallback, long timeout)方法完成消息的发送处理，其中请求参数的默认值：communicationMode= CommunicationMode.SYNC（同步发送方式），sendCallback=null，timeout=3000。大致逻辑如下：
     * 1、检查DefaultMQProducerImpl的ServiceState是否为RUNNING，若不是RUNNING状态则直接抛出MQClientException异常给调用者；
     * <p>
     * 2、校验Message消息对象的各个字段的合法性，其中Message对象的body的长度不能大于128KB；
     * <p>
     * 3、以Message消息中的topic为参数调用DefaultMQProducerImpl. tryToFindTopicPublishInfo (String topic)方法从topicPublishInfoTable: ConcurrentHashMap<String/* topic , TopicPublishInfo>变量中获取TopicPublishInfo对象；
     * 3.1）若没有对应的TopicPublishInfo对象，则创建一个新的TopicPublishInfo对象，并存入topicPublishInfoTable变量中（在该变量中有该topic的记录，表示该Producer在向NameServer获取到topic信息之后会为该topic创建本地消息队列MessageQueue对象），然后调用MQClientInstance.updateTopicRouteInfoFromNameServer(String topic)方法更新刚创建的TopicPublishInfo对象的变量值；对于第一次向NameServer获取该topic的TopicPublishInfo对象肯定是获取不到的，故该TopicPublishInfo对象的haveTopicRouterInfo等于false；
     * 3.2）检查返回的TopicPublishInfo对象的haveTopicRouterInfo和messageQueueList: List<MessageQueue>队列，若haveTopicRouterInfo等于true或者TopicPublishInfo对象的messageQueueList:ListList<MessageQueue>变量不为空，则直接返回该TopicPublishInfo对象；
     * 3.3）否则调用MQClientInstance.updateTopicRouteInfoFromNameServer (String topic, boolean isDefault, DefaultMQProducer defaultMQProducer)方法获取默认topic值"TBW102"的TopicPublishInfo对象，其中isDefault=true，defaultMQProducer等于自身的defaultMQProducer对象，在第一次向NameServer获取该topic的TopicPublishInfo对象时会出现此情况。若Broker开启了自动创建Topic功能，则在启动Broker时会自动创建TBW102的主题，不建议开启此功能，而应该采用手工创建的方式创建TOPIC；
     * <p>
     * 4、若上一步获取的TopicPublishInfo对象不为空，并且该对象的List<MessageQueue>队列也不为空，则执行下面的消息发送逻辑，否则抛出MQClientException异常，表示该topic在本地没有创建消息队列；
     * <p>
     * 5、设置消息重试次数以及超时时间，分别由参数DefaultMQProducer.retryTimesWhenSendFailed（默认为2）和DefaultMQProducer.sendMsgTimeout（默认为3000毫秒）设置；重试次数为retryTimesWhenSendFailed+1，超时时间为sendMsgTimeout+1000；当发送消息失败，在重试次数小于3和未到达4秒的超时时间，则再次发送消息；
     * <p>
     * 6、选择发送的Broker和QueueId，即选择MessageQueue对象。调用TopicPublishInfo.selectOneMessageQueue(String lastBrokerName)方法选择MessageQueue对象，若该消息是第一次发送则从TopicPublishInfo.messageQueueList:List<MessageQueue>中选择上一次发送消息使用的MessageQueue对象后面的一个MessageQueue对象，若已经是最后一个则又从列表第一个MessageQueue对象开始选择；（由TopicPublishInfo.sendWhichQueue来记录此次发送消息选择的是第几个MessageQueue对象）；否则选择上一次发送消息使用的MessageQueue对象后面的一个与上次不同BrokerName的MessageQueue对象；
     * <p>
     * 7、调用sendKernelImpl(Message msg, MessageQueue mq, CommunicationMode communicationMode, SendCallback sendCallback, long timeout)进行消息的发送工作；
     * 7.1）根据入参MessageQueue对象的brokerName从MQClientInstance.brokerAddrTable获取主用Broker（BrokerID=0）的地址；
     * 7.2）若该Broker地址为空，则再次调用tryToFindTopicPublishInfo(String topic)方法（详见第3步操作）从topicPublishInfoTable变量中获取TopicPublishInfo对象，然后在执行第7.1的操作获取主用Broker地址；若仍然为空则抛出MQClientException异常；否则继续下面的操作；
     * 7.3）若消息内容大于规定的消息内容大小（由DefaultMQProducer.compressMsgBodyOverHowmuch参数指定，默认是4KB）之后就使用java.util.zip.DeflaterOutputStream进行对消息内容进行压缩；
     * 7.4）若消息内容经过压缩则置sysFlag标志位从右往左第1个字节为1；若消息的property属性中TRAN_MSG字段不为空，并且是可解析为true的字符串，则将sysFlag标志位第3个字节为1；
     * 7.5）在应用层实现CheckForbiddenHook接口，并可以调用DefaultMQProducerImpl.RegisterCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook)方法来注册CheckForbiddenHook钩子，可以实现CheckForbiddenHook接口，该接口的方法是在消息发送前检查是否属于禁止消息。在此先检查是否注册CheckForbiddenHook钩子，若注册了则执行；
     * 7.6)在应用层实现SendMessageHook接口可以调用DefaultMQProducerImpl.registerSendMessageHook(SendMessageHook hook)方法注册SendMessageHook钩子，实现该接口的类有两个方法，分别是消息发送前和消息发送后的调用。在此先检查是否注册SendMessageHook钩子，若注册了则执行sendMessageBefore方法；在发送结束之后在调用sendMessageAfter方法；
     * 7.7)构建SendMessageRequestHeader对象，其中，该对象的defaultTopic变量值等于"TBW102", defaultTopicQueueNums变量值等于DefaultMQProducer.defaultTopicQueueNums值，默认为4，queueId等于MessageQueue对象的queueId；
     * 7.8）调用MQClientAPIImpl.sendMessage(String addr, String brokerName,Message msg, SendMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, SendCallback sendCallback)进行消息的发送；在此方法中发送请求码为SEND_MESSAGE的RemotingCommand消息；根据通信模式（communicationMode=同步[SYNC]、异步[ASYNC]、单向[ONEWAY]）分别调用不同的方法，将消息内容发送到Broker。为了降低网络传输数量，设计了两种SendMessageRequestHeader对象，一种是对象的变量名用字母简写替代，类名是SendMessageRequestHeaderV2，一种是对象的变量名是完整的，类名是SendMessageRequestHeader。
     * <p>
     * A）同步[SYNC]：在调用发送消息的方法之后，同步等待响应消息，响应信息达到之后调用MQClientAPIImpl.processSendResponse(String brokerName, Message msg, RemotingCommand response)方法处理响应消息，返回给上层调用者；
     * B）异步[ASYNC]：在发送消息之前，首先创建了内部匿名InvokeCallback类并实现operationComplete方法，并初始化ResponseFuture对象，其中InvokeCallback匿名类就是该对象的InvokeCallback变量值； 然后将该ResponseFuture对象以请求ID存入NettyRemotingAbstract.ResponseTable: ConcurrentHashMap<Integer /* opaque , ResponseFuture>变量中；最后在收到响应消息之后以响应ID（即请求ID）从NettyRemotingAbstract. ResponseTable变量中取ResponseFuture对象，然后调用InvokeCallback类的operationComplete方法，完成回调工作；
     * C）单向[ONEWAY]：只负责将消息发送出去，不接受响应消息；
     * <p>
     * 8、对于同步方式发送消息，若未发送成功，并且Producer设置允许选择另一个Broker进行发送（参数DefaultMQProducer. retryAnotherBrokerWhenNotStoreOK指定，默认为false，不允许），则从第5步的检查发送失败次数和发送时间是否已经超过阀值开始重新执行；否则直接返回；
     * </p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(
            Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg     Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * Send message to broker asynchronously.
     * <p>
     * 异步发送消息到broker
     * </p>
     * <p>
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
     * </p>
     * <p>
     * Similar to {@link #send(Message)}, internal implementation would potentially retry up to
     * {@link #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication
     * and application developers are the one to resolve this potential issue.
     *
     * @param msg          Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg,
                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     *
     * @param msg          message to send.
     * @param sendCallback Callback to execute.
     * @param timeout      send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * @param msg Message to send.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     *
     * @param msg Message to send.
     * @param mq  Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq);
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     *
     * @param msg     Message to send.
     * @param mq      Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue specified.
     *
     * @param msg          Message to send.
     * @param mq           Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
     *
     * @param msg          Message to send.
     * @param mq           Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout      Send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq  Target message queue.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
                           MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, mq);
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg      Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg      Argument to work along with message queue selector.
     * @param timeout  Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout specified.
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout      Send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg      Argument used along with message queue selector.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * This method is to send transactional messages.
     * <p>
     * 发送事务消息
     *
     * @param msg          Transactional message to send.
     * @param tranExecuter local transaction executor.
     * @param arg          Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
                                                          final Object arg)
            throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * Create a topic on broker.
     *
     * @param key      accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * Create a topic on broker.
     *
     * @param key          accesskey
     * @param newTopic     topic name
     * @param queueNum     topic's queue number
     * @param topicSysFlag topic system flag
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * Search consume queue offset of the given time stamp.
     *
     * @param mq        Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(mq, timestamp);
    }

    /**
     * Query maximum offset of the given message queue.
     *
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(mq);
    }

    /**
     * Query minimum offset of the given message queue.
     *
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(mq);
    }

    /**
     * Query earliest message store time.
     *
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(mq);
    }

    /**
     * Query message of the given offset message ID.
     *
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException    if there is any broker error.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(
            String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * Query message by key.
     *
     * @param topic  message topic
     * @param key    message key index word
     * @param maxNum max message number
     * @param begin  from when
     * @param end    to when
     * @return QueryResult instance contains matched messages.
     * @throws MQClientException    if there is any client error.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     *
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException    if there is any broker error.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(String topic,
                                  String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(topic, msgId);
    }

    @Override
    public SendResult send(
            Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs,
                           MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }
}
