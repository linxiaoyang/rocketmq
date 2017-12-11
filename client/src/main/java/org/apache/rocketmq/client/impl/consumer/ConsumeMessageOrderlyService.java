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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;


/**
 * 对于顺序消费的三把锁：1）首先在ConsumeMessageOrderlyService类中定义了定时任务每隔20秒执行一次lockMQPeriodically()方法，获取该Consumer端在Broker端锁住的MessageQueue集合（即分布式锁），并将RebalanceImpl.processQueueTable:ConcurrentHashMap<MessageQueue, ProcessQueue>集合中获得分布式锁的MessageQueue对象（消费队列）对应的ProcessQueue对象（消费处理队列）加上本地锁（即该对象的lock等于ture）以及加锁的时间，目的是为了在消费时在本地检查消费队列是否锁住；2）在进行消息队列的消费过程中，对MessageQueue对象进行本地同步锁，保证同一时间只允许一个线程消息一个ConsumeQueue队列；3）在回调业务层定义的ConsumeMessageOrderlyService.messageListener:MessageListenerOrderly类的consumeMessage方法之前获取ProcessQueue.lockConsume:ReentrantLock变量的锁即消费处理队列的锁，该锁的粒度比消息队列的同步锁粒度更小，该锁的目的是保证在消费的过程中不会被解锁。
 * <p>
 * 3.1 回调业务层定义的消费方法
 * <p>
 * 在回调DefaultMQPushConsumerImpl.pullMessage方法中的内部类PullCallback.onSucess方法时，调用ConsumeMessageOrderlyService. submitConsumeRequest方法提交消费请求ConsumeRequest对象，该消费请求就是在ConsumeMessageOrderlyService类的内部定义了ConsumeRequest线程类，将此对象提交到ConsumeMessageOrderlyService类的线程池中，由该线程完成回调业务层定义的消费方法，该内部线程类的run方法逻辑如下：
 * <p>
 * 1、检查ProcessQueue.dropped是否为true，若不是则直接返回；
 * <p>
 * 2、对MessageQueue对象加锁，保证同一时间只允许一个线程使用该MessageQueue对象。调用ConsumeMessageOrderlyService.messageQueueLock. fetchLockObject(MessageQueue mq)获取锁对象。下面的处理逻辑均在获得该Object对象的互斥锁（synchronized）后进行处理；从而保证了在并发情况下一个MessageQueue对象只有一个线程使用，大致逻辑为：
 * <p>
 * 2.1）根据MessageQueue对象从MessageQueueLock.mqLockTable: ConcurrentHashMap<MessageQueue, Object>中获取Object对象，若获取到Object对象不为null，则直接返回该对象；
 * <p>
 * 2.2）若获取的Object对象为null，则创建一个Object对象，并保存到mqLockTable变量中，为了防止并发采用putIfAbsent方法存入该列表中，若已经有该MessageQueue对象，则返回已经存在的Object对象，若不为空，则返回该已存在的Object对象；
 * <p>
 * 3、若消息模式是广播或者对应的ProcessQueue.locked等于true且锁的时间未过期（根据获取锁locked的时候设置的lastLockTimestamp值来判断）,则初始化局部变量continueConsume=true，然后无限期的循环执行下面的逻辑，直到局部变量continueConsume=false为止或者跳出循环，便终止了该方法的执行，否则执行第4步操作；
 * <p>
 * 3.1)执行for循环下面的逻辑，直到该continueConsume等于false或者直接跳出循环为止；
 * <p>
 * 3.2）检查ProcessQueue.dropped是否为true，若不是则跳出循环；
 * <p>
 * 3.3）若消息模式是集群并且ProcessQueue.locked不等于true（未锁住）或者锁住了但是锁已经超时，则调用ConsumeMessageOrderlyService.tryLockLaterAndReconsume(MessageQueue mq, ProcessQueue processQueue, long delayMills)方法，在该方法中初始化一个线程并将线程放入ConsumeMessageOrderlyService. scheduledExecutorService线程池中，然后跳出循环。该线程在延迟delayMills毫秒之后被执行，该线程的功能是获取MessageQueue队列的分布式锁，然后再调用ConsumeMessageOrderlyService.submitConsumeRequestLater (MessageQueue mq, ProcessQueue processQueue, long delayMills)方法。
 * <p>
 * 3.4）若该循环从开始到现在连续执行的时间已经超过的最大连续执行值（由property属性中的"rocketmq.client.maxTimeConsumeContinuously"设定，默认为60秒），则调用ConsumeMessageOrderlyService. submitConsumeRequestLater(MessageQueue mq, ProcessQueue processQueue, long delayMills)方法，其中delayMills=100，表示在100毫秒之后再调用ConsumeMessageOrderlyService.submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume)方法，在该方法中重新创建ConsumeRequest对象，并放入ConsumeMessageOrderlyService.consumeExecutor线程池中重新被消费；
 * <p>
 * 3.4）获取一次批量消费的消息个数batchSize（由参数DefaultMQPushConsumer.consumeMessageBatchMaxSize指定，默认为1），然后调用ProcessQueue.takeMessags(int batchSize)，在该方法中，从ProcessQueue.msgTreeMap变量中获取batchSize个数的List<MessageExt>列表；并且从msgTreeMap中删除，存入临时变量msgTreeMapTemp中，返回List<MessageExt>列表，若该列表为空，表示该msgTreeMap列表中的消息已经消费完了，置ProcessQueue.consuming等于false；
 * <p>
 * 3.5）检查返回的List<MessageExt>列表是否为空，若为空，则置局部变量continueConsume=false表示不在循环执行，本次消息队列的消费结束；否则继续执行下面的步骤；
 * <p>
 * 3.6）检查DefaultMQPushConsumerImpl.consumeMessageHookList: ArrayList<ConsumeMessageHook>是否为空，若不是，则初始化ConsumeMessageContext对象，并调用ArrayList<ConsumeMessageHook>列表中每个ConsumeMessageHook对象的consumeMessageBefore (ConsumeMessageContextcontext)方法，该ArrayList<ConsumeMessageHook>列表由业务层调用 DefaultMQPushConsumerImpl.registerConsumeMessageHook (ConsumeMessageHook hook)方法设置；
 * <p>
 * 3.7）执行业务层定义的消费消息的业务逻辑并返回消费结果。先调用ProcessQueue. lockConsume:ReentrantLock变量的lock方法获取锁（目的是防止在消费的过程中，被其他线程将此消费队列解锁了，从而引起并发消费的问题），然后调用ConsumeMessageOrderlyService. messageListener:MessageListenerOrderly类的consumeMessage方法，保证同一个ProcessQueue在同一时间只能有一个线程调用consumeMessage方法，由应用层实现该MessageListenerOrderly接口的consumeMessage方法，执行完成之后调用调用ProcessQueue. lockConsume:ReentrantLock变量的unlock方法释放锁；
 * <p>
 * 3.8）当consumeMessage方法的返回值status为空时，将结果状态status赋值为ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT；
 * <p>
 * 3.9）检查DefaultMQPushConsumerImpl.consumeMessageHookList: ArrayList<ConsumeMessageHook>是否为空，若不是，则初始化ConsumeMessageContext对象，并调用ArrayList<ConsumeMessageHook>列表中每个ConsumeMessageHook对象的consumeMessageAfter (ConsumeMessageContextcontext)方法，该ArrayList<ConsumeMessageHook>列表由业务层调用DefaultMQPushConsumerImpl
 * <p>
 * .registerConsumeMessageHook(ConsumeMessageHook hook)方法设置；
 * <p>
 * 3.10）处理回调方法consumeMessage的消费结果，并将消费结果赋值给变量continueConsume。调用ConsumeMessageOrderlyService. processConsumeResult(List<MessageExt> msgs, ConsumeOrderlyStatus status, ConsumeOrderlyContext context, ConsumeRequest consumeRequest)方法，详见5.7.2小节；
 * <p>
 * 3.11）继续从第3.1步开始遍历，不间断地从ProcessQueue对象的List<MessageExt>列表中获取消息对象并消费；直到消费完为止；
 * <p>
 * 4、检查ProcessQueue.dropped是否为true，若不为true则直接返回，否则调用ConsumeMessageOrderlyService.tryLockLaterAndReconsume (MessageQueue mq, ProcessQueue processQueue, long delayMills)方法，其中delayMills=100，即在100毫秒之后重新获取锁后再次进行消费；
 * <p>
 * 3.2 根据消费结果进行相应处理
 * <p>
 * 以回调应用层定义的ConsumeMessageOrderlyService.consumeMessage方法的返回处理结果为参数调用ConsumeMessageOrderlyService. processConsumeResult(List<MessageExt> msgs, ConsumeOrderlyStatus status, ConsumeOrderlyContext context, ConsumeRequest consumeRequest) 方法。大致逻辑如下：
 * <p>
 * 1、若ConsumeOrderlyContext.autoCommit为true（默认为true，可以在应用层的实现类MessageListenerOrderly的consumeMessage方法中设置该值）；根据consumeMessage方法返回的不同结果执行不同的逻辑：
 * <p>
 * 1.1）若status等于SUCCESS，则调用ProcessQueue.commit()方法，大致逻辑如下：
 * <p>
 * A）获取ProcessQueue.lockTreeMap:ReentrantReadWriteLock锁对该方法的整个处理逻辑加锁；
 * <p>
 * B）从msgTreeMapTemp（在从msgTreeMap中获取消息并消费时存入的）中获取最后一个元素的key值，即最大的offset；
 * <p>
 * C）将ProcessQueue.msgCount值减去临时变量msgTreeMapTemp中的个数，即表示剩下的未消费的消息个数；
 * <p>
 * D）清空临时变量msgTreeMapTemp列表的值；
 * <p>
 * E)返回最大的offset+1的值并赋值给局部变量commitOffset值用于更新消费进度之用；
 * <p>
 * F）然后调用ConsumerStatsManager.incConsumeOKTPS(String group, String topic, long msgs)方法进行消费统计；
 * <p>
 * 1.2）若status等于SUSPEND_CURRENT_QUEUE_A_MOMENT，稍后重新消费。大致逻辑如下：
 * <p>
 * 1.2.1）首先调用ProcessQueue.makeMessageToCosumeAgain (List<MessageExt> msgs)方法将List<MessageExt>列表的对象重新放入msgTreeMap变量中。大致逻辑如下：
 * <p>
 * A）获取ProcessQueue.lockTreeMap:ReentrantReadWriteLock锁对该方法的整个处理逻辑加锁；
 * <p>
 * B）将List<MessageExt>列表的对象重新放入msgTreeMap变量中。遍历List<MessageExt>列表的每个MessageExt对象，以该对象的queueoffset值为key值从msgTreeMapTemp中删除对应的MessageExt对象；将MessageExt对象以MessageExt对象的queueoffset为key值重新加入到ProcessQueue.msgTreeMap变量中；
 * <p>
 * 1.2.2）然后调用ConsumeMessageOrderlyService. submitConsumeRequestLater(ProcessQueue processQueue, MessageQueue messageQueue, long suspendTimeMillis)方法，其中suspendTimeMillis由ConsumeOrderlyContext.suspendCurrentQueueTimeMillis变量在应用层的回调方法中设定，默认为1000，表示在1000毫秒之后调用ConsumeMessageOrderlyService.submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume)方法，在该方法中重新创建ConsumeRequest对象，并放入ConsumeMessageOrderlyService.consumeExecutor线程池中；
 * <p>
 * 1.2.3）置局部变量continueConsume=false；
 * <p>
 * 1.2.4）调用ConsumerStatsManager.IncConsumeFailedTPS方法进行消费统计；
 * <p>
 * 2、若ConsumeOrderlyContext.autoCommit为false，根据consumeMessage方法返回的不同状态执行不同的逻辑：
 * <p>
 * 2.1）若status等于SUCCESS，仅调用ConsumerStatsManager.incConsumeOKTPS(String group, String topic, long msgs)方法进行消费统计，并未调用ProcessQueue.commit()方法清理队列数据；
 * <p>
 * 2.2）若status等于COMMIT时，才调用ProcessQueue.commit()方法，并返回消费最大的offset值并赋值给局部变量commitOffset值用于更新消费进度之用；
 * <p>
 * 2.3）若status等于ROLLBACK，表示要重新消费，
 * <p>
 * A）首先调用ProcessQueue.rollback()方法，将msgTreeMapTemp变量中的内容全部重新放入msgTreeMap变量中，同时清理msgTreeMapTemp变量；
 * <p>
 * B）然后调用ConsumeMessageOrderlyService.submitConsumeRequestLater (ProcessQueue processQueue, MessageQueue messageQueue, long suspendTimeMillis)方法，其中suspendTimeMillis由ConsumeOrderlyContext. suspendCurrentQueueTimeMillis变量在应用层的回调方法中设定，默认为1000，表示在1000毫秒之后调用ConsumeMessageOrderlyService. submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume)方法，在该方法中重新创建ConsumeRequest对象，并放入ConsumeMessageOrderlyService. consumeExecutor线程池中；
 * <p>
 * C）置局部变量continueConsume=false；
 * <p>
 * 2.4）若status等于SUSPEND_CURRENT_QUEUE_A_MOMENT，将msgTreeMapTemp变量中的内容全部重新放入msgTreeMap变量中，然后结束此轮消费（continueConsume=false），等待1秒之后再次提交消费，大致逻辑如下：
 * <p>
 * A）首先调用ProcessQueue.makeMessageToCosumeAgain(List<MessageExt> msgs)方法，详见1.2.1步；
 * <p>
 * B）然后调用ConsumeMessageOrderlyService.submitConsumeRequestLater (ProcessQueue processQueue, MessageQueue messageQueue, long suspendTimeMillis)方法，其中suspendTimeMillis由ConsumeOrderlyContext. suspendCurrentQueueTimeMillis变量在应用层的回调方法中设定，默认为1000；
 * <p>
 * C）置局部变量continueConsume=false；
 * <p>
 * D）调用ConsumerStatsManager.IncConsumeFailedTPS方法进行消费统计；
 * <p>
 * 3、若上面两步得到的commitOffset值大于0，则调用OffsetStore.updateOffset(MessageQueue mq, long offset, boolean increaseOnly)方法更新消费进度。对于LocalFileOffsetStore或RemoteBrokerOffsetStore类，该方法逻辑是一样的，以MessageQueue对象为key值从offsetTable: ConcurrentHashMap<MessageQueue, AtomicLong>变量中获取values值，若该values值为空，则将MessageQueue对象以及commitOffset值存入offsetTable变量中，若不为空，则比较已经存在的值，若大于已存在的值才更新；
 * <p>
 * 4、返回continueConsume值；若该值为false则结束此轮消费。
 * <p>
 * 3.3 重新获取分布式锁后再消费（tryLockLaterAndReconsume）
 * <p>
 * 在集群模式下，若ProcessQueue未锁或者锁已经超时，则调用ConsumeMessageOrderlyService.tryLockLaterAndReconsume(MessageQueue mq, ProcessQueue processQueue, long delayMills)方法从Broker重新获取锁之后再进行消费。
 * <p>
 * 在该方法中初始化一个Runnable匿名线程，并在delayMills毫秒之后再执行该匿名线程，该匿名线程的run方法逻辑如下：
 * <p>
 * 1、先调用ConsumeMessageOrderlyService.lockOneMQ(MessageQueue mq)方法获取MessageQueue队列的锁，向该MessageQueue对象的brokerName下面的主用Broker发送LOCK_BATCH_MQ请求码的请求消息，请求Broker将发送的MessageQueue对象锁住；若该请求的MessageQueue对象在Broker返回的锁住集合中，则锁住成功了；
 * <p>
 * 2、调用ConsumeMessageOrderlyService.submitConsumeRequestLater(ProcessQueue processQueue, MessageQueue messageQueue, long suspendTimeMillis)方法，若锁住成功则suspendTimeMillis=10；若未锁住，则suspendTimeMillis=3000。
 * <p>
 * 3、在ConsumeMessageOrderlyService.submitConsumeRequestLater方法中，初始化一个匿名的Runnable线程类，在suspendTimeMillis毫秒之后，执行该线程类，在该类的run方法中调用ConsumeMessageOrderlyService.submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume)方法，其中dispathToConsume=true；在该方法中根据ProcessQueue 和MessageQueue对象初始化ConsumeRequest对象，并放入ConsumeMessageOrderlyService.consumeExecutor线程池中；
 * <p>
 * 4 并发消费（ConsumeMessageConcurrentlyService）
 * <p>
 * 4.1 回调业务层定义的消费方法
 * <p>
 * 在回调DefaultMQPushConsumerImpl.pullMessage方法中的内部类PullCallback.onSucess方法时，调用ConsumeMessageConcurrentlyService.submitConsumeRequest方法提交消费请求ConsumeRequest对象，该消费请求对象是在ConsumeMessageConcurrentlyService类的内部定义了ConsumeRequest线程类；将此对象提交到ConsumeMessageConcurrentlyService类的线程池中，由该线程完成回调业务层定义的消费方法，该内部线程类的run方法逻辑如下：
 * <p>
 * 1、检查ProcessQueue.dropped是否为true，若不是则直接返回；
 * <p>
 * 2、检查DefaultMQPushConsumerImpl.consumeMessageHookList: ArrayList<ConsumeMessageHook>是否为空，若不是，则初始化ConsumeMessageContext对象，并调用ArrayList<ConsumeMessageHook>列表中每个ConsumeMessageHook对象的consumeMessageBefore (ConsumeMessageContext context)方法，该consumeMessageHookList变量由业务层调用DefaultMQPushConsumerImpl.registerConsumeMessageHook (ConsumeMessageHook hook)方法设置，由业务层自定义ConsumeMessageHook接口的实现类，实现该接口的consumeMessageBefore(final ConsumeMessageContext context)和consumeMessageAfter(final ConsumeMessageContext context)方法，分别在回调业务层的具体消费方法之前和之后调用者两个方法。
 * <p>
 * 3、遍历List<MessageExt>列表，检查每个消息（MessageExt对象）中topic值，若该值等于"%RETRY%+consumerGroup"，则从消息的propertis属性中获取"RETRY_TOPIC"属性的值，若该属性值不为null，则将该属性值赋值给该消息的topic值；对于重试消息，会将真正的topic值放入该属性中；
 * <p>
 * 4、调用ConsumeMessageConcurrentlyService.messageListener: MessageListenerConcurrently的consumeMessage方法；该messageListener变量是由DefaultMQPushConsumer.registerMessageListener (MessageListener messageListener)方法在业务层设置的，对于并发消费，则在业务层就要实现MessageListenerConcurrently接口的consumeMessage方法；该回调方法consumeMessage返回ConsumeConcurrentlyStatus. CONSUME_SUCCESS或者ConsumeConcurrentlyStatus.RECONSUME_LATER值；
 * <p>
 * 5、检查上一部分返回值status，若为null，则置status等于ConsumeConcurrentlyStatus.RECONSUME_LATER；
 * <p>
 * 5、检查DefaultMQPushConsumerImpl.consumeMessageHookList: ArrayList<ConsumeMessageHook>是否为空，若不是，则初始化ConsumeMessageContext对象，并调用ArrayList<ConsumeMessageHook>列表中每个ConsumeMessageHook对象的executeHookAfter(ConsumeMessageContext context)方法，该ArrayList<ConsumeMessageHook>列表是应用层设置的回调类；
 * <p>
 * 6、处理消费失败的消息。对于回调方法consumeMessage的执行结果为失败的消息，以内部Producer的名义重发到Broker端用于重试消费。若ProcessQueue.dropped为false，调用ConsumeMessageConcurrentlyService. processConsumeResult(ConsumeConcurrentlyStatus status, ConsumeConcurrentlyContext context, ConsumeRequest consumeRequest)方法，该方法的大致逻辑如下：
 * <p>
 * 6.1）由于ConsumeConcurrentlyContext.ackIndex初始的默认值为Integer.MAX_VALUE，表示消费成功的个数，该变量值可由业务层在执行回调方法失败之后重新设定；若status等于CONSUME_SUCCESS则判断ackIndex是否大于ConsumeRequest中MessageExt列表的个数，如果大于则表示该列表的消息全部消费成功，则将ackIndex置为List<MessageExt>列表的个数减1，如果小于则ackIndex表示消费失败的消息在列表的位置；然后对消费成功/失败个数进行统计；若status等于RECONSUME_LATER则表示消息全部消费失败，置ackIndex=-1，并对消费失败个数进行统计；
 * <p>
 * 6.2）若此消息模式为广播（DefaultMQPushConsumer.messageModel设置），则从消费失败的列表位置（ackIndex+1）开始遍历List<MessageExt>列表，打印消费失败的MessageExt日志信息；
 * <p>
 * 6.3）若此消息模式为集群（DefaultMQPushConsumer.messageModel设置），则从消费失败的列表位置（ackIndex+1）开始遍历List<MessageExt>列表，对于消费失败的MessageExt对象，调用 ConsumeMessageConcurrentlyService.sendMessageBack (MessageExt msg, ConsumeConcurrentlyContext context)方法向Broker发送CONSUMER_SEND_MSG_BACK请求码；对于发送BACK消息失败之后发送RETRY消息也失败的MessageExt消息构建一个临时列表msgBackFailedList；
 * <p>
 * 6.4）检查上一步的临时列表msgBackFailedList是否为空，若不为空则说明有发送重试消息失败的，则首先从ConsumeRequest.msgs: List<MessageExt>变量列表中删除消费失败的这部分消息，然后调用ConsumeMessageConcurrentlyService.submitConsumeRequestLater(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue)方法，在该方法中初始化一个匿名线程，并将该线程放入调度线程池（ConsumeMessageConcurrentlyService.scheduledExecutorService）中，延迟5秒之后执行该匿名线程，该匿名线程的run方法调用ConsumeMessageConcurrentlyService.submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispatchToConsume)方法，将发送BACK消息失败之后发送RETRY消息也失败的List<MessageExt>列表封装成ConsumeRequest线程对象，再次提交到ConsumeMessageConcurrentlyService.consumeExecutor线程池中；
 * <p>
 * 6.5）将消费成功了的消息（ConsumeRequest.msgs列表中剩下的）从 ProcessQueue.msgTreeMap列表中删除，同时更新 ProcessQueue.msgCount并返回该列表中第一个MessageExt元素的key值，即该元素的queueoffset值；
 * <p>
 * 6.6）若上一步返回的queueoffset大于等于0，则调用 OffsetStore.updateOffset( MessageQueue mq, long offset, boolean increaseOnly)更新该消费队列的消费进度；
 * <p>
 * 4.2 对消费失败的信息发送重试消息给Broker（用于消息重试）
 * <p>
 * 在业务层消费消息失败并且是集群模式下，会调用ConsumeMessageConcurrentlyService.sendMessageBack(MessageExt msg, ConsumeConcurrentlyContext context)方法。在该方法中调用DefaultMQPushConsumerImpl.sendMessageBack(MessageExt msg,int delayLevel, String brokerName)方法，大致逻辑如下：
 * <p>
 * 1、根据brokerName获取Broker地址；
 * <p>
 * 2、调用MQClientAPIImpl.consumerSendMessageBack(String addr, MessageExt msg, String consumerGroup, int delayLevel, long timeoutMillis)方法，其中delayLevel参数由ConsumeConcurrentlyContext. delayLevelWhenNextConsume可在业务层的回调方法中设置，默认为0（表示由服务器根据重试次数自动叠加）；构建ConsumerSendMsgBackRequestHeader对象，其中该对象的offset等于MessageExt.commitLogOffset、originTopic等于MessageExt.topic、originMsgId等于MessageExt.msgId；然后发送CONSUMER_SEND_MSG_BACK请求码的信息给Broker（详见3.1.17小节）；
 * <p>
 * 3、如果发送重试消息出现异常，则构建以%RETRY%+consumerGroup为topic值的新Message消息,构建过程如下：
 * <p>
 * 3.1)从MessageExt消息的properties属性中获取"ORIGIN_MESSAGE_ID"属性值，若没有则以MessageExt消息的msgId来设置新Message信息的properties属性的ORIGIN_MESSAGE_ID属性值，若有该属性值则以该属性值来设置新Message信息的properties属性的ORIGIN_MESSAGE_ID属性值。保证同一个消息有多次发送失败能获取到真正消息的msgId；
 * <p>
 * 3.2）将MessageExt消息的flag赋值给新Message信息的flag值；
 * <p>
 * 3.3）将MessageExt消息的topic值存入新Message信息的properties属性中"RETRY_TOPIC"属性的值；
 * <p>
 * 3.4）每次消费重试将MessageExt消息的properties属性中"RECONSUME_TIME"值加1；然后将该值再加3之后存入新Message信息的properties属性中"DELAY"属性的值；
 * <p>
 * 4、调用在启动Consumer时创建的名为"CLIENT_INNER_PRODUCER"的DefaultMQProducer对象的send(Message msg)方法，以Consumer内部的消息Producer重发消息（该消息是消费失败且回传给Broker也失败的）；
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageOrderlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
                                         final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
            final List<MessageExt> msgs,
            final ConsumeOrderlyStatus status,
            final ConsumeOrderlyContext context,
            final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());
                case SUCCESS:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume; ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        List<MessageExt> msgs = this.processQueue.takeMessags(consumeBatchSize);
                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                        .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                this.processQueue.getLockConsume().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                    break;
                                }

                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                                hasException = true;
                            } finally {
                                this.processQueue.getLockConsume().unlock();
                            }

                            if (null == status
                                    || ConsumeOrderlyStatus.ROLLBACK == status
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                        .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                    .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            continueConsume = false;
                        }
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
