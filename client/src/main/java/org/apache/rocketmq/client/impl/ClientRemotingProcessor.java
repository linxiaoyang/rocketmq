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
package org.apache.rocketmq.client.impl;

import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;

public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE:
                return this.checkTransactionState(ctx, request);
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return this.notifyConsumerIdsChanged(ctx, request);
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return this.getConsumeStatus(ctx, request);

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }


    /**
     * 1、根据收到的事务消息的properties中的"PGROUP"参数值获取该事务消息是由哪个producerGroup发出的，即获取producerGroup值；
     * <p>
     * 2、以producerGroup值从MQClientInstance.producerTable中获取MQProducerInner对象；
     * <p>
     * 3、根据获取请求消息的渠道解析远程Broker的地址；
     * <p>
     * 4、调用DefaultMQProducerImpl.checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader header)方法检查事务消息的状态。在该方法中初始化一个Runnable线程，然后将该线程放入线程池中，由该线程类处理相关逻辑：
     * <p>
     * 4.1）获取DefaultMQProducerImpl对象的TransactionCheckListener
     * <p>
     * 值，该值由应用层调用TransactionMQProducer.setTransactionCheckListener (TransactionCheckListener transactionCheckListener)方法设置；在应用层实现TransactionCheckListener接口的checkLocalTransactionState方法，该方法中检查本地的业务是否处理成功，根据处理结果来决定已发送到Broker的事务消息是应该提交还是回滚；该方法返回LocalTransactionState. ROLLBACK_MESSAGE或者LocalTransactionState.COMMIT_MESSAGE或LocalTransactionState.UNKNOW；
     * <p>
     * 4.2）构建EndTransactionRequestHeader对象，该对象的fromTransactionCheck变量等于true、tranStateTableOffset变量等于收到到的消息的TranStateTableOffset值；若上一步返回LocalTransactionState. COMMIT_MESSAGE，则commitOrRollback值等于TransactionCommitType，若上一步返回LocalTransactionState. ROLLBACK_MESSAGE，则commitOrRollback值等于TransactionRollbackType；若上一步返回LocalTransactionState.UNKNOW，则commitOrRollback值等于TransactionNotType；
     * <p>
     * 4.3）向发送CHECK_TRANSACTION_STATE请求码的Broker发送END_TRANSACTION请求码，将该事务消息的处理结果告知Broker；
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx,
                                                 RemotingCommand request) throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader =
                (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    public RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx,
                                                    RemotingCommand request) throws RemotingCommandException {
        try {
            final NotifyConsumerIdsChangedRequestHeader requestHeader =
                    (NotifyConsumerIdsChangedRequestHeader) request.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
            log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.getConsumerGroup());
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
        }
        return null;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx,
                                       RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
                (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
                requestHeader.getTimestamp());
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
        if (request.getBody() != null) {
            ResetOffsetBody body = ResetOffsetBody.decode(request.getBody(), ResetOffsetBody.class);
            offsetTable = body.getOffsetTable();
        }
        this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
        return null;
    }

    @Deprecated
    public RemotingCommand getConsumeStatus(ChannelHandlerContext ctx,
                                            RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerStatusRequestHeader requestHeader =
                (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        Map<MessageQueue, Long> offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
        GetConsumerStatusBody body = new GetConsumerStatusBody();
        body.setMessageQueueTable(offsetTable);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerRunningInfoRequestHeader requestHeader =
                (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        ConsumerRunningInfo consumerRunningInfo = this.mqClientFactory.consumerRunningInfo(requestHeader.getConsumerGroup());
        if (null != consumerRunningInfo) {
            if (requestHeader.isJstackEnable()) {
                Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
                String jstack = UtilAll.jstack(map);
                consumerRunningInfo.setJstack(jstack);
            }

            response.setCode(ResponseCode.SUCCESS);
            response.setBody(consumerRunningInfo.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }

        return response;
    }

    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumeMessageDirectlyResultRequestHeader requestHeader =
                (ConsumeMessageDirectlyResultRequestHeader) request
                        .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        final MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(request.getBody()));

        ConsumeMessageDirectlyResult result =
                this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.getConsumerGroup(), requestHeader.getBrokerName());

        if (null != result) {
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(result.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }

        return response;
    }
}
