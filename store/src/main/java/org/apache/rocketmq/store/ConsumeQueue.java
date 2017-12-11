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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consumequeue类对应的是每个topic和queuId下面的所有文件。
 * Consumequeue类文件的存储路径默认为$HOME/store/consumequeue/{topic}/{queueId}/{fileName}，
 * 每个文件由30W条数据组成，每条数据的结构如下图所示：commitlog offset(8)|size(4)|message tag hashcode(8)
 * 消息的起始物理偏移量physical offset(long 8字节)+消息大小size(int 4字节)+tagsCode(long 8字节)，每条数据的大小为20个字节，从而每个文件的默认大小为600万个字节。
 * <p>
 * 每个cosumequeue文件的名称fileName，名字长度为20位，左边补零，剩余为起始偏移量；比如00000000000000000000代表了第一个文件，起始偏移量为0，文件大小为600W，当第一个文件满之后创建的第二个文件的名字为00000000000006000000，起始偏移量为6000000，以此类推，第三个文件名字为00000000000012000000，起始偏移量为12000000，消息存储的时候会顺序写入文件，当文件满了，写入下一个文件。
 */
public class ConsumeQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;

        /**
         * 创建mapped file queue
         */
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 恢复ConsumeQueue内存数据
     * <p>
     * 主要是更新ConsumeQueue.maxPhysicOffset变量（最后一个消息对应的物理offset）和删除最后一个
     * 消息所在文件之后的文件以及对应的MapedFile对象。
     * <p>
     * 在Broker启动过程中会调用该方法。首先获取ConsumeQueue对象中的List<MappedFile>列表变量，
     * 从倒数第三个文件开始恢复，即列表中的倒数第三个MappedFile对象；若文件总数没有达到3个，则从
     * 第一个文件开始恢复。从选择的开始恢复的文件（倒数第三个或者第一个文件）中顺序解析consumequeue
     * 的每个数据单元（即20个字节）。
     * <p>
     * 正常情况下ConsumeQueue文件中二进制信息的内容是连续的20个字节的信息单元组成，并且每个信息单元
     * 中的offset和size都是大于零的。从倒数第三个文件中开始解析，若整个文件的所有数据单元均有效
     * （即offset和size均大于零）则继续获取下一个文件的MapedFile对象然后从文件头开始解析数据单元，
     * 直到遇到文件的一个数据单元的offset和size不大于零为止。以最后一个有效的消息单元的前8个字节
     * （即为物理位移offset）来初始化ConsumeQueue.maxPhysicOffset变量；
     * <p>
     * 计算出processOffset,等于最后一个MappedFile文件的fileFromOffset（即文件名的数字值）+最后一个
     * 有效的消息单元在该MappedFile对象中的位置偏移量mappedFileOffest；
     * <p>
     * 调用MapedFileQueue.truncateDirtyFiles(long processOffset)方法，利用该方法首先更新
     * processOffset所在文件对象（MapedFile对象）的wrotepostion和committedpostion值，然后
     * 清理指定偏移量processOffset所在文件之后的所有文件，不仅删除物理文件而且删除MappedFile列表
     * 中对应的MapedFile对象。
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 查找消息发送时间最接近timestamp逻辑队列的offset
     * <p>
     * <p>
     * 首先利用请求时间戳timestamp调用MapedFileQueue对象的getMapedFileByTime方法获取文件的更新时间在该时间戳之后的文件对应的MapedFile对象。
     * <p>
     * 然后用二分查找法查找最接近timestamp的消息的逻辑队列。大致逻辑如下：
     * <p>
     * 置low=0、high=该文件已经写入的最后消息位置；然后(low+high)/2计算逻辑队列的中间消息单元的偏移量，然后读取以该偏移量开始一个消息单元，并从该消息单元中解析物理偏移量；
     * <p>
     * 以该物理偏移量调用从commitlog中获取消息内容并取该消息的发送时间字段值；
     * <p>
     * 若该发送时间值小于请求时间戳timestamp，则low标记为该中间位置；
     * <p>
     * 若该发送时间大于请求时间戳timestamp，则high标记为该中间位置；
     * <p>
     * 然后重复第一步的操作，直到找到消息发送时间等于请求时间戳timestamp或者high<low为止。
     * <p>
     * 若上述二分法一直在往左边寻找且未找到或者一直在往右边寻找且未找到，则将最后一个中间位置的offset作为最接近的值；
     * <p>
     * 若上述二分法左右均选择过且未找到发送时间等于请求时间戳timestamp的消息，则取距请求时间戳timestamp最近的消息在逻辑队列中的offset值；
     *
     * @param timestamp
     * @return
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 删除指定偏移量之后的逻辑文件
     * <p>
     * 此步的目的是首先删除指定偏移量processOffset所在文件之后的逻辑文件，
     * 其次是修正所在文件对应的MappedFile对象的写入位置和提交位置两个变量。
     * <p>
     * 从ConsumeQueue.MapedFileQueue的MapedFile队列中获取最后一个
     * MappedFile文件（调用），然后从第一个数据块开始解析：
     * <p>
     * 若第一块消息单元中的物理偏移值（第1至8个字节）大于了processOffset，
     * 则从MapedFile列表中删除该记录并从磁盘中删除物理文件，然后继续获取列
     * 表的最后一个文件（即为当初的倒数第二个文件了）并从第一个数据块开始解析。
     * 否则不断地更新MappedFile文件wrotePostion和CommittedPosition两个变
     * 量为解析到的数据块位置；直到解析到数据块的大小为空或者物理偏移值大于了processOffset为止。
     *
     * @param phyOffet
     */
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet - 1;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    /**
     * 获取最后一条消息对应物理队列的下一个偏移量
     * <p>
     * <p>
     * 首先调用MapedFileQueue对象的getLastMapedFile2方法获取列表中的最后一个MapedFile对象（即对应的是最后一个物理文件）；
     * <p>
     * 然后读取最后一个文件的最有一个有效的消息单元，取该消息单元的commitlogoffset（第1至8个字节）和消息大小size（第9至12个字节）；
     * <p>
     * 最后将commitlogOffset+size即为物理队列中的下一个消息的偏移量。
     *
     * @return
     */
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 根据物理队列最小offset计算修正逻辑队列最小offset
     * <p>
     * 调用correctMinOffset(long phyMinOffset)方法，根据物理队列的最小offset来修正逻辑队列的最小offset值。大致逻辑如下：
     * <p>
     * 1）获取MapedFileQueue队列中第一个MapedFile对象，然后调用selectMapedBuffer(0)方法从第0位开始读取所有数据；
     * <p>
     * 2）逐个解析获取到的数据的数据单元，检查每个消息单元中保存的物理偏移量（前8字节）是否大于等于最小物理偏移量，若是则将此消息单元的位置加上该文件的起始偏移量作为ConsumeQueue数据的最小逻辑偏移量存于变量minLogicOffset中。
     *
     * @param phyMinOffset
     */
    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                    this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }


    /**
     * 将commitlog物理偏移量/消息大小等信息直接写入consumequeue中
     * <p>
     * 调用ConsumeQueue.putMessagePostionInfoWrapper(long offset, int size, long tagsCode, long storeTimestamp, long logicOffset)方法，将相关参数写入Consumequeue文件中。具体步骤如下：
     * <p>
     * 1）根据入参信息构建consumequeue中的数据格式buffer块；
     * <p>
     * 2）找到最后一个MappedFile文件，调用appendMessage(final byte[] data)方法将consumequeue的格式数据追加到MappedFile文件的后面；更新StoreCheckpoint.logicsMsgTimestamp变量值为请求对象中的storeTimestamp值；
     *
     * @param request
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                            topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                    request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {

        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxPhysicOffset = offset;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 根据消息序号索引获取consumequeue数据
     * <p>
     * 调用getIndexBuffer(final long startIndex)方法，在方法中，由于startIndex只是消息单元的序号，
     * 故需将startIndex乘以20方可得到起始偏移量offset，然后调用MapedFileQueue对象的
     * findMapedFileByteOffset方法根据该起始偏移量找到所在的文件，即对应的MapedFile对象，然后调用
     * MapedFile对象selectMapedBuffer(int pos)，其中pos等于起始偏移量offset除以文件大小（fileSize）的余数。
     *
     * @param startIndex
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    /**
     * 获取指定位置所在文件的下一个文件的起始偏移量
     *
     *
     * 调用rollNextFile(long offset)方法，获取该入参的偏移量所在文件的下一个文件的起始偏移量，
     * 算法如下：offset+mapedFileSzie/20-offset% mapedFileSzie,
     * 即得到下个偏移量的值，该值要乘以消息单元大小（20）才是真正的文件读取偏移量位置。
     * @param index
     * @return
     */
    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
