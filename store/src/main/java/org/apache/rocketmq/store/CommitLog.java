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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store all metadata downtime for recovery, data protection reliability
 * <p>
 * commitlog文件的存储地址：$HOME\store\commitlog\${fileName}，每个文件的大小
 * 默认1G =1024*1024*1024，commitlog的文件名fileName，名字长度为20位，左边补零，
 * 剩余为起始偏移量；比如00000000000000000000代表了第一个文件，起始偏移量为0，
 * 文件大小为1G=1073741824；当这个文件满了，第二个文件名字为00000000001073741824，
 * 起始偏移量为1073741824，以此类推，第三个文件名字为00000000002147483648，起始
 * 偏移量为2147483648 消息存储的时候会顺序写入文件，当文件满了，写入下一个文件
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    private final MappedFileQueue mappedFileQueue;
    private final DefaultMessageStore defaultMessageStore;
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitLogService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    private volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;
    private final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
        this.defaultMessageStore = defaultMessageStore;

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.commitLogService = new CommitRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 获取最大物理偏移量
     * <p>
     * 调用MapedFileQueue类的方法获取在MapedFile队列中的最大Offset值，即为当前写入消息的最大位置
     *
     * @return
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     * <p>
     * 读取指定起始位置offset所在文件的全部剩余消息
     * <p>
     * <p>
     * 调用MapedFileQueue的findMapedFileByOffset方法取指定起始位置offset所在的文件对应的MapedFile对象。
     * 然后计算在该文件内部的起始位置，由于参数中的指定起始位置是从第一个文件开始位置算起的，针对文件内部的起
     * 始位置应该是offset%fileSize。最后调用MapedFile对象的selectMapedBuffer方法获取该文件中从起始位置
     * 开始的所有剩余信息。
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     * 正常恢复CommitLog内存数据
     * <p>
     * <p>
     * 主要是恢复MapedFileQueue对象的commitedWhere变量值（即刷盘的位置），删除该
     * commitedWhere值所在文件之后的commitlog文件以及对应的MapedFile对象。
     * <p>
     * 在Broker启动过程中会调用该方法。从MapedFileQueue的MapedFile列表的倒数第三个对象
     * （即倒数第三个文件）开始遍历每块消息单元，若总共没有三个文件，则从第一个文件开始遍历每块消息单元。
     * <p>
     * 首先，每次读取到消息单元块之后，进行CRC的校验，在校验过程中，若检查到第5至8字节MAGICCODE字段等于
     * BlankMagicCode（cbd43194）则返回msgSize=0的DispatchRequest对象；若校验未通过或者读取到的信息
     * 为空则返回msgSize=-1的DispatchRequest对象；否则返回msgSize等于第1个4字节的msgSize字段值的
     * DispatchRequest对象。
     * <p>
     * 对于msgSize大于零，则读取的偏移量mapedFileOffset累加msgSize；若等于零，则表示读取到了文件的最后一块信息，
     * 则继续读取下一个MapedFile对象的文件；直到消息的CRC校验未通过或者读取完所有信息为止。
     * <p>
     * 计算有效信息的最后位置processOffset，计算方式为：取最后读取的MapedFile对象的
     * fileFromOffset加上最后读取的位置mapedFileOffset值。
     * <p>
     * 最后更新内存中的对象数据信息：设置MapedFileQueue对象的commitedWhere等于processOffset；
     * 调用truncateDirtyFiles方法将processOffset所在文件之后的文件全部清理掉，并且将所在文件
     * 对应的MapedFile对象的wrotepostion和commitPosition设置为processOffset%fileSize，
     * 即等于mapedFileOffset值。
     */
    public void recoverNormally() {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            if (log.isDebugEnabled()) {
                log.debug(String.valueOf(obj.hashCode()));
            }
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            // 1 TOTAL SIZE  代表这个消息的大小
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE  魔数
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }


            byte[] bytesContent = new byte[totalSize];
            /**
             * 消息体BODY CRC 当broker重启recover时会校验
             */
            int bodyCRC = byteBuffer.getInt();
            /**
             * 队列的ID
             */
            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            /**
             * 这个值是个自增值不是真正的consume queue的偏移量，
             * 可以代表这个consumeQueue队列或者tranStateTable
             * 队列中消息的个数，若是非事务消息或者commit事务消息，
             * 可以通过这个值查找到consumeQueue中数据，QUEUEOFFSET * 20才
             * 是偏移地址；若是PREPARED或者Rollback事务，则可以
             * 通过该值从tranStateTable中查找数据
             */
            long queueOffset = byteBuffer.getLong();

            /**
             * 代表消息在commitLog中的物理起始地址偏移量
             */
            long physicOffset = byteBuffer.getLong();

            /**
             * 指明消息是事物事物状态等消息特征，二进制为四个字节从右往左数：
             * 当4个字节均为0（值为0）时表示非事务消息；当第1个字节为1
             * （值为1）时表示表示消息是压缩的（Compressed）；当第2个
             * 字节为1（值为2）表示多消息（MultiTags）；当第3个字节为1
             * （值为4）时表示prepared消息；当第4个字节为1（值为8）时
             * 表示commit消息；当第3/4个字节均为1时（值为12）时表示
             * rollback消息；当第3/4个字节均为0时表示非事务消息；
             */
            int sysFlag = byteBuffer.getInt();

            /**
             * 消息产生端(producer)的时间戳
             */
            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);

            /**
             * 消息在broker存储时间
             */
            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);
            /**
             * 消息被某个订阅组重新消费了几次（订阅组之间独立计数）,因为重试消息发送到了topic名字
             * 为%retry%groupName的队列queueId=0的队列中去了，成功消费一次记录为0；
             */
            int reconsumeTimes = byteBuffer.getInt();
            /**
             * 表示是prepared状态的事物消息
             */
            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            /**
             * topic名称内容大小
             */
            byte topicLen = byteBuffer.get();
            /**
             * 将byteBuffer中的数据放到bytesContent中
             */
            byteBuffer.get(bytesContent, 0, topicLen);
            /**
             * topic的内容值
             */
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            /**
             * 属性值大小
             */
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                /**
                 * 将byteBuffer中的数据放到bytesContent
                 */
                byteBuffer.get(bytesContent, 0, propertiesLength);
                /**
                 * 将byte转化成String
                 */
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                /**
                 * 从String转化成Map
                 */
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }


    /**
     * 在Broker启动过程中会调用该方法。与正常恢复的区别在于：正常恢复是从倒数第3个文件开始恢复；而异常恢复是从最后的文件开始往前寻找与checkpoint文件的记录相匹配的一个文件。
     * <p>
     * 首先，若该MapedFile队列为空，则MapedFileQueue对象的commitedWhere等于零，并且调用DefaultMessageStore.destroyLogics() 方法删除掉逻辑队列consumequeue中的物理文件以及清理内存数据。否则从MapedFileQueue的MapedFile列表找到从哪个文件（对应的MapedFile对象）开始恢复数据，查找逻辑如下：
     * <p>
     * 1、从MapedFile列表中的最后一个对象开始往前遍历每个MapedFile对象，检查该MapedFile对象对应的文件是否满足恢复条件，查找逻辑如下，若查找完整个队列未找到符合条件的MapedFile对象，则从第一个文件开始恢复。
     * <p>
     * A）从该文件中获取第一个消息单元的第5至8字节的MAGICCODE字段，若该字段等于MessageMagicCode（即不是正常的消息内容），则直接返回后继续检查前一个文件；
     * <p>
     * B）获取第一个消息单元的第56位开始的8个字节的storeTimeStamp字段，若等于零，也直接返回后继续检查前一个文件；
     * <p>
     * C）检查是否开启消息索引功能（MessageStoreConfig .messageIndexEnable，默认为true）并且是否使用安全的消息索引功能（MessageStoreConfig. MessageIndexSafe，默认为false，在可靠模式下，异常宕机恢复慢；非可靠模式下，异常宕机恢复快），若开启可靠模式下面的消息索引，则消息的storeTimeStamp字段表示的时间戳必须小于checkpoint文件中物理队列消息时间戳、逻辑队列消息时间戳、索引队列消息时间戳这三个时间戳中最小值，才满足恢复数据的条件；否则消息的storeTimeStamp字段表示的时间戳必须小于checkpoint文件中物理队列消息时间戳、逻辑队列消息时间戳这两个时间戳中最小值才满足恢复数据的条件；
     * <p>
     * 2、从找到的MapedFile对象开始往后开始遍历每个文件的消息单元，首先，每次读取到消息单元块之后，进行CRC的校验，在校验过程中，若检查到第5至8字节MAGICCODE字段等于BlankMagicCode（cbd43194）则返回msgSize=0的DispatchRequest对象；若校验未通过或者读取到的信息为空则返回msgSize=-1的DispatchRequest对象；否则返回msgSize等于第1个4字节的msgSize字段值的DispatchRequest对象。
     * <p>
     * 对于msgSize大于零，则读取的偏移量mapedFileOffset累加msgSize，并将DispatchRequest对象放入DefaultMessageStore.DispatchMessageService服务线程中，由该线程在后台进行ConsumeQueue队列和Index服务的数据加载；若等于零，则表示读取到了文件的最后一块信息，则继续读取下一个MapedFile对象的文件；直到消息的CRC校验未通过或者读取完所有信息为止。
     * <p>
     * 计算有效信息的最后位置processOffset，计算方式为：取最后读取的MapedFile对象的fileFromOffset加上最后读取的位置mapedFileOffset值。
     * <p>
     * 3、更新内存中的对象数据信息：设置MapedFileQueue对象的commitedWhere等于processOffset；调用MapedFileQueue.truncateDirtyFiles方法将processOffset所在文件之后的文件全部清理掉，并且将所在文件对应的MapedFile对象的wrotepostion和commitPosition设置为processOffset%fileSize，即等于mapedFileOffset值。
     * <p>
     * 4、调用DefaultMessageStore.truncateDirtyLogicFiles(long processOffset) 方法，在该方法中遍历DefaultMessageStore.consumeQueueTable的values值（即ConcurrentHashMap<Integer/* queueId ,ConsumeQueue>），对于调用每个ConsumeQueue对象的truncateDirtyLogicFiles(long processOffset)方法，该方法根据物理偏移值processOffset删除无效的逻辑文件。
     */
    public void recoverAbnormally() {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            /**
             * 从MapedFile列表中的最后一个对象开始往前遍历每个MapedFile对象，检查该MapedFile对象对应的文件是否满足恢复条件，
             * 查找逻辑如下，若查找完整个队列未找到符合条件的MapedFile对象，则从第一个文件开始恢复
             */
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                // Normal data
                if (size > 0) {
                    mappedFileOffset += size;

                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                        if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else {
                        this.defaultMessageStore.doDispatch(dispatchRequest);
                    }
                }
                // Intermediate file read error
                else if (size == -1) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // Commitlog case files are deleted
        /**
         * 若该MapedFile队列为空，则MapedFileQueue对象的commitedWhere等于零，
         * 并且调用DefaultMessageStore.destroyLogics() 方法删除掉逻辑队列
         * consumequeue中的物理文件以及清理内存数据
         */
        else {
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * 在Broker接受到生产者的消息之后，会间接的调用CommitLog.putMessage(MessageExtBrokerInner msg)方法完成消息的写入操作。具体逻辑如下：
     * <p>
     * 1、获取消息的sysflag字段，检查消息是否是非事务性（第3/4字节为0）或者提交事务（commit，第4字节为1，第3字节为0）消息，
     * 若是，再从消息properties属性中获取"DELAY"参数（该参数在应用层通过Message.setDelayTimeLevel(int level)方法设置，
     * 消息延时投递时间级别，0表示不延时，大于0表示特定延时级别）属性的值（即延迟级别），若该值大于0，则将此消息设置为定时消息；
     * 即更改该消息为定时消息，更改如下信息：第一，将MessageExtBrokerInner对象的topic值更改为"SCHEDULE_TOPIC_XXXX"，
     * 第二，根据延迟级别获取延时消息的队列ID（queueId等于延迟级别减去1）并更改queueId值；第三，将消息中原真实的topic
     * 和queueId存入消息属性中；第四，该延迟消息的consumequeue队列中的tagscode等于存储时间加上延迟级别对应的时长，若没有
     * 该延迟级别则将存储时间加上1000；
     * <p>
     * 2、调用MapedFileQueue.getLastMapedFile方法获取或者创建最后一个文件（即MapedFile列表中的最后一个MapedFile对象），
     * 若还没有文件或者已有的最后一个文件已经写满则创建一个新的文件，即创建一个新的MapedFile对象并返回；
     * <p>
     * 3、调用MapedFile.appendMessage(Object msg, AppendMessageCallback cb)方法将消息内容写入MapedFile.mappedByteBuffer：
     * MappedByteBuffer对象，即写入消息缓存中；由后台服务线程定时的将缓存中的消息刷盘到物理文件中；
     * <p>
     * 4、若最后一个MapedFile剩余空间不足够写入此次的消息内容，即返回状态为END_OF_FILE标记，则再次调用MapedFileQueue.getLastMapedFile方法获取新的MapedFile对象然后调用MapedFile.appendMessage方法重写写入，最后继续执行后续处理操作；若为PUT_OK标记则继续后续处理；若为其他标记则返回错误信息给上层；
     * <p>
     * 5、初始化DispatchRequest对象，其中包括topic、queueID、wroteOffset（写入的开始物理位置）、wroteBytes（写入的大小）、logicsOffset（已经写入的消息块个数）、消息key值等；调用DefaultMessageStore.DispatchMessageService.putRequest(DispatchRequest dispatchRequest)将请求消息放入DispatchMessageService.requestsWrite队列中；由DispatchMessageService服务处理该请求；为请求中的信息创建consumequeue数据和index索引。
     * <p>
     * 6、若该Broker是同步刷盘，并且消息的property属性中"WAIT"参数(该参数在应用层可以通过Message.setWaitStoreMsgOK(boolean waitStoreMsgOK)方法设置，表示是否等待服务器将消息存储完毕再返回（可能是等待刷盘完成或者等待同步复制到其他服务器）)为空或者为TRUE，则利用GroupCommitService后台线程服务进行刷盘操作，具体步骤如下：
     * <p>
     * 1）构建GroupCommitRequest对象，其中nextOffset变量的值等于wroteOffset（写入的开始物理位置）加上wroteBytes（写入的大小）,表示下一次写入消息的开始位置；
     * <p>
     * 2）将该对象存入GroupCommitService.requestsWrite写请求队列中，并唤醒GroupCommitService线程将写队列的数据与读队列的数据交互（读队列的数据肯定是空）；
     * <p>
     * 3）该线程的doCommit方法中遍历读队列的数据，检查MapedFileQueue.committedWhere（刷盘刷到哪里的记录）是否大于等于GroupCommitRequest.nextOffset，若是表示该请求消息表示nextOffset之前的消息已经被刷盘，否则调用CommitLog.MapedFileQueue.commit(int flushLeastPages) 进行刷盘操作；
     * <p>
     * 4）用MapedFileQueue的存储时间戳storeTimestamp变量值（在MapedFileQueue.commit方法成功执行后更新）更新StoreCheckpoint.physicMsgTimestamp变量值（checkpoint文件内容中其中一个值）；
     * <p>
     * 5）清空读请求队列requestRead；
     * <p>
     * 7、若该Broker为异步刷盘（ASYNC_FLUSH），唤醒FlushRealTimeService线程服务。在该线程的run方法处理逻辑如下：
     * <p>
     * 1）若根据CommitLog刷盘间隔时间（默认是1秒）来间断性的调用CommitLog.MapedFileQueue.commit(int flushLeastPages)方法进行刷盘操作；
     * <p>
     * 2）用MapedFileQueue的存储时间戳storeTimestamp变量值（在MapedFileQueue.commit方法成功执行后更新）更新StoreCheckpoint.physicMsgTimestamp变量值（checkpoint文件内容中其中一个值）；
     * <p>
     * 8、若该Broker为同步双写主用（SYNC_MASTER），并且消息的property属性中"WAIT"参数为空或者为TRUE，则等待监听主Broker将数据同步到从Broker的结果，若同步失败，则置PutMessageResult对象的putMessageStatus变量为FLUSH_SLAVE_TIMEOUT，监测方法如下：
     * <p>
     * 1）检查主从数据传输是否正常。备用连接是否大于0，主用put的位置masterPutwhere等于wroteOffset（写入的开始物理位置）加上wroteBytes（写入的大小），masterPutwhere减去HAService.push2SlaveMaxOffset（写入到Slave的最大Offset）的差值不能大于256M，否则视为主备同步异常，置PutMessageResult对象的putMessageStatus变量为SLAVE_NOT_AVAILABLE；
     * <p>
     * 2)若主备同步正常，则利用wroteOffset（写入的开始物理位置）加上wroteBytes（写入的大小）的值为参数构建GroupCommitRequest对象，即该对象的nextOffset值等于wroteOffset+wroteBytes；然后调用HAService.GroupTransferService.putRequest(GroupCommitRequest request)方法将请求对象放入 GroupTransferService服务的队列中，用于监听是否同步完成；再调用GroupCommitRequest.waitForFlush(long timeout)方法，该方法一直处于阻塞状态，直到HAService线程服务完成同步工作或者超时才返回结果；若GroupCommitRequest对象的flushOK变量为true则表示同步成功了，在GroupTransferService服务线程中判断是否同步完成的方法是用该对象中的nextOffset值与HAService.push2SlaveMaxOffset比较。
     * <p>
     * 9、返回PutMessageResult对象；若上诉都成功则该对象的putMessageStatus变量为PUT_OK；
     *
     * @param msg
     * @return
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, msg);
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // Synchronization flush
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK()) {
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                            + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                service.wakeup();
            }
        }
        // Asynchronous flush
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    boolean flushOK =
                            request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                                + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取最小Offset
     * <p>
     * <p>
     * 从MapedFileQueue中获取第一个MapedFile对象（即第一个文件），若该文件可用
     * （MapedFile对象的availabe变量值）则返回该对象的fileFromOffset值，若不可用，
     * 则取下一个文件的起始偏移量，计算方式为：
     * fileFromOffset值+文件的固定大小1G-fileFromOffset%1G。
     * fileFromOffset%1G一般情况下为0
     *
     * @return
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 该方法的入参有两个：读取的起始偏移量offset和读取的大小
     * size。首先调用findMapedFileByOffset方法根据起始偏移
     * 量offset所在的MapedFile对象；然后调用MapedFile对象的
     * selectMapedBuffer方法获取从offset开始的size大小的消
     * 息内容；由于offset是commitlog文件的全局偏移量，要以
     * offset%mapedFileSize的余数作
     * 为单个文件的起始读取位置传入selectMapedBuffer方法中
     *
     * @param offset
     * @param size
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 获取指定位置所在文件的下一个文件的起始偏移量
     * <p>
     * 调用rollNextFile(long offset)方法，获取该入参的偏移量所在文件的下一个
     * 文件的起始偏移量，算法如下：offset+mapedFileSzie-offset% mapedFileSzie
     *
     * @param offset
     * @return
     */
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 根据入参中的指定位置startOffset值调用getLastMapedFile方法获取
     * 最后一个MapedFile对象，若MapedFile队列为空则以startOffset为起
     * 始偏移量创建新的文件（即新的MapedFile对象）。然后调用MapedFile对
     * 象的appendMessage(final byte[] data)方法将二进制消息写入缓存中。
     *
     * @param startOffset
     * @param data
     * @return
     */
    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                            if (!flushOK) {
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }

                        req.wakeupCustomer(flushOK);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 该类实现了AppendMessageCallback回调类的doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,final int maxBlank, final Object msg)方法，该顺序写方法的具体逻辑如下：
     * <p>
     * 1）获取当前内存对象的写入位置（wrotePostion变量值）；若写入位置没有超过文件大小则继续顺序写入；
     * <p>
     * 2）由内存对象mappedByteBuffer创建一个指向同一块内存的ByteBuffer对象，并将内存对象的写入指针指向写入位置；
     * <p>
     * 3）以文件的起始偏移量（fileFromOffset）、ByteBuffer对象、该内存对象剩余的空间（fileSize-wrotePostion）、消息对象msg为参数调用AppendMessageCallback回调类的doAppend方法；
     * <p>
     * 3.1）根据broker的ip+port和在整个commitlog上开始位置（fileFromOffset+wrotepostion）生成16个字节的消息ID（messageId）；
     * <p>
     * 3.2）根据消息的"topic-queueid"作为key值在CommitLog. topicQueueTable中获取values值（即为queueoffset值），若没有则置为0并以此"topic-queueid"作为key值存入topicQueueTable中；
     * <p>
     * 3.3）通过参数sysflag的标志位判断消息类型，若是PREPARED事务，则取TransactionStateService.tranStateTableOffset变量值（DefaultMessageStore中初始化的TransactionStateService对象）赋值给queueoffset临时变量；若是ROLLBACK事务，则取消息中的queueoffset值赋值给临时变量queueoffset；
     * <p>
     * 3.4)根据消息对象msg计算消息的总长度，若总长度大于消息长度的最大值（MessageStoreConfig.maxMessageSize指定，默认为512K）则返回由status= MESSAGE_SIZE_EXCEEDED 初始化的AppendMessageResult对象；若总长度加上文件末尾剩余的空格8字节的值大于该内存对象剩余的空间，则将剩余的空间用空格填满，即向内存对象中写入剩余空间大小、结尾文件MAGICCODE（cbd43194），内存对象剩余的部分填充空格，并返回由status=END_OF_FILE、写入的字节数（等于剩余空间大小）、消息ID、queueoffset初始化的AppendMessageResult对象；
     * <p>
     * 3.5）将消息内容按照commitlog规定的格式先写入DefaultAppendMessageCallback对象创建的内存对象msgIdMemory中，然后在整体复制到MapedFile的内存对象mappedByteBuffer中，返回由status=PUT_OK、写入的字节数（等于剩余空间大小）、消息ID、queueoffset初始化的AppendMessageResult对象；
     * <p>
     * 3.6）若非事务性或者提交事务消息，在写入完成之后将对应的values值加1；若是PREPARED事务，则将TransactionStateService.tranStateTableOffset变量值加1，该变量值表示在statetable文件中的消息个数；
     * <p>
     * 4）将MapedFile.wrotePostion的值加上写入的字节数（AppendMessageResult对象返回的值）；
     * <p>
     * 5）更新存储时间戳MapedFile.storeTimestamp ；
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;
        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }


        /**
         * @param fileFromOffset 文件的起始偏移量
         * @param byteBuffer     ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
         *                       byteBuffer.position(currentPos);
         * @param maxBlank       该内存对象剩余的空间（fileSize-wrotePostion）
         * @param msgInner       消息对象msg
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            /**
             *
             *
             *
             * fileFromOffset 可以看做是这个文件的初始偏移量
             * byteBuffer.position 可以看做是这个文件的的可写的位置
             */
            long wroteOffset = fileFromOffset + byteBuffer.position();

            this.resetByteBuffer(hostHolder, 8);
            /**
             * 根据broker的ip+port和在整个commitlog上开始位置（fileFromOffset+wrotepostion）生成16个字节的消息ID（messageId）；
             */
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            /**
             * 拼接topicQueueTable的key
             */
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             *
             * 将属性搞成byte类型的
             */
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            /**
             * 得出属性 字节数组的大小
             */
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            /**
             * topic的名称的字节数组
             */
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            /**
             * topic的名称的字节数组的大小
             */
            final int topicLength = topicData.length;

            /**
             * 消息体的字节数组的长度
             */
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            /**
             * 计算传递过来消息的总的大小
             */
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            /**
             * maxBlank 是最后一个文件剩余的容量大小
             */
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            /**
             * 查队列里面数据的偏移量
             */
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();
            this.resetByteBuffer(hostHolder, 8);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(hostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(bodyLen, topicLength, propertiesLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(hostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
