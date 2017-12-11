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

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;


/**
 * 事务消息的发送
 *
 *
 * 该事务消息是指Producer端的业务逻辑处理与向MQ发送消息事件是在同一个事务里面，即这两件事件要么同时成功要么同时失败。
 可以解决如下类似场景的问题：A用户和B用户的账户体系不在同一台服务器上面，现在A用户向B用户转账100元，为了提高执行效率，就采用消息队列的方式实现异步处理。大致逻辑是A用户扣款100元，然后发送消息给消息队列，B用户的程序从队列中获取转账信息并向B用户上账100元。
 若A用户先扣款再发送消息，如果扣款成功而发送消息失败，则导致数据不一致；
 若先发送消息再扣款，但是发送消息成功而扣款失败，也会导致数据不一致；
 若采用RocketMQ发送事务消息，则第一阶段发送Prepared消息时，会拿到消息的地址，第二阶段执行本地事物，第三阶段通过第一阶段拿到的地址去访问消息，并修改状态。如果第三阶段的确认消息发送失败了，RocketMQ会定期扫描消息集群中的事物消息，发现Prepared消息，它会向消息发送者确认，RocketMQ会根据发送端设置的策略来决定是回滚还是继续发送确认消息。这样就保证了消息发送与本地事务同时成功或同时失败。
 在应用层通过初始化TransactionMQProducer类，可以发送事务消息，大致逻辑如下：
 1、自定义业务类，该类实现TransactionCheckListener接口的checkLocalTransactionState(final MessageExt msg)方法，该方法是对于本地业务执行完毕之后发送事务消息状态时失败导致Broker端的事务消息一直处于PREPARED状态的补救，Broker对于长期处于PREPARED状态的事务消息发起回查请求时，Producer在收到回查事务消息状态请求之后，调用该checkLocalTransactionState方法，该方法的请求参数是之前发送的事务消息，在该方法中根据此前发送的事务消息来检查该消息对应的本地业务逻辑处理的情况，根据处理情况告知Broker该事务消息的最终状态（commit或者rollback）；
 2、自定义执行本地业务逻辑的类，该类实现LocalTransactionExecuter接口的executeLocalTransactionBranch(final Message msg, final
 Object arg)方法，在该方法中执行本地业务逻辑，根据业务逻辑执行情况反馈事务消息的状态（commit或者rollback）；
 3、初始化TransactionMQProducer类，将第1步中的类赋值给TransactionMQProducer.transactionCheckListener变量；设置检查事务状态的线程池中线程的最大值、最小值、队列数等参数值；
 4、调用TransactionMQProducer.start方法启动Producer，该启动过程与普通的Producer启动区别不大，首先调用DefaultMQProducerImpl.initTransactionEnv()方法初始化检查事务状态的线程池，将第三阶段的事务消息确认的逻辑放入该线程池中执行；然后调用DefaultMQProducer.start()启动Producer；
 5、构建事务消息Message对象，然后与第2步创建的执行本地业务逻辑类为参数调用TransactionMQProducer.sendMessageInTransaction(Message msg,LocalTransactionExecuter tranExecuter, Object arg)方法。在方法中首先检查TransactionMQProducer.transactionCheckListener变量是否为空，若没有定义（即为空）则直接抛出异常，表示该回查事务消息状态类必须定义；然后调用DefaultMQProducerImpl.sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, Object arg)方法。在该方法中分为三阶段，大致逻辑如下：
 5.1）将事务消息Message对象的properties属性中的“TRAN_MSG”字段设为true，“PGROUP”字段设为该Producer的producerGroup值；然后调用DefaultMQProducerImpl.send(Message msg)方法将该事务消息发送到Broker；如果发送失败时抛出异常，则不继续下面的处理逻辑，直接跑异常到应用层；否则继续下面的处理；
 5.2）若发送消息的返回结果（SendResult对象）状态为SEND_OK，则调用业务层实现的executeLocalTransactionBranch方法，执行本地业务逻辑并返回本地事务状态LocalTransactionState；
 5.3）若发送消息的返回结果（SendResult对象）状态不是SEND_OK，则不执行本地业务逻辑，直接将本地事务状态LocalTransactionState置为ROLLBACK_MESSAGE；
 5.5）调用DefaultMQProducerImpl.endTransaction(SendResult sendResult, LocalTransactionState localTransactionState, Throwable localException)方法，在该方法中：
 A）创建EndTransactionRequestHeader对象，该对象的commitOrRollback变量是根据上一步的本地业务逻辑处理结果来设置（可能为MessageSysFlag.TransactionCommitType/MessageSysFlag.TransactionRollbackTypeMessageSysFlag.TransactionNotType）、tranStateTableOffset变量值等于发送消息之后返回的该消息的queueoffset值、msgId变量值等于发送消息之后由Broker返回的MessageId；
 B）调用MQClientAPIImpl.endTransactionOneway(String addr, EndTransactionRequestHeader requestHeader, String remark, long timeoutMillis)方法向Broker发送END_TRANSACTION请求码；
 */
public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;
    private int checkThreadPoolMinSize = 1;
    private int checkThreadPoolMaxSize = 1;
    private int checkRequestHoldMax = 2000;

    public TransactionMQProducer() {
    }

    public TransactionMQProducer(final String producerGroup) {
        super(producerGroup);
    }

    public TransactionMQProducer(final String producerGroup, RPCHook rpcHook) {
        super(producerGroup, rpcHook);
    }

    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.initTransactionEnv();
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException {
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter, arg);
    }

    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }

    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }

    public int getCheckThreadPoolMinSize() {
        return checkThreadPoolMinSize;
    }

    public void setCheckThreadPoolMinSize(int checkThreadPoolMinSize) {
        this.checkThreadPoolMinSize = checkThreadPoolMinSize;
    }

    public int getCheckThreadPoolMaxSize() {
        return checkThreadPoolMaxSize;
    }

    public void setCheckThreadPoolMaxSize(int checkThreadPoolMaxSize) {
        this.checkThreadPoolMaxSize = checkThreadPoolMaxSize;
    }

    public int getCheckRequestHoldMax() {
        return checkRequestHoldMax;
    }

    public void setCheckRequestHoldMax(int checkRequestHoldMax) {
        this.checkRequestHoldMax = checkRequestHoldMax;
    }
}
