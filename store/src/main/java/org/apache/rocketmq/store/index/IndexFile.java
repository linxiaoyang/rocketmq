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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为操作Index文件提供访问服务,Index文件的存储位置是：$HOME \store\index\${fileName}，
 * 文件名fileName是以创建时的时间戳命名的，
 * 文件大小是固定的，等于40+500W*4+2000W*20= 420000040个字节大小。
 * <p>
 * <p>
 * Index Header结构各字段的含义：
 * <p>
 * beginTimestamp：第一个索引消息落在Broker的时间戳；
 * <p>
 * endTimestamp：最后一个索引消息落在Broker的时间戳；
 * <p>
 * beginPhyOffset：第一个索引消息在commitlog的偏移量；
 * <p>
 * endPhyOffset：最后一个索引消息在commitlog的偏移量；
 * <p>
 * hashSlotCount：构建索引占用的槽位数；
 * <p>
 * indexCount：构建的索引个数；
 * <p>
 * Slot Table里面的每一项保存的是这个topic-key是第几个索引；根据topic-key的Hash值除以500W取余得到这个Slot Table的序列号，然后将此索引的顺序个数存入此Table中。
 * <p>
 * Slottable的位置（absSlotPos）的计算公式：40+keyHash%（500W）*4；
 * <p>
 * Index Linked List的字段含义：
 * <p>
 * keyHash:topic-key(key是消息的key)的Hash值；
 * <p>
 * phyOffset:commitLog真实的物理位移；
 * <p>
 * timeOffset：时间位移，消息的存储时间与Index Header中beginTimestamp的时间差；
 * <p>
 * slotValue：当topic-key(key是消息的key)的Hash值取500W的余之后得到的Slot Table的slot位置中已经有值了（即Hash值取余后在Slot Table中有冲突时），则会用最新的Index值覆盖，并且将上一个值写入最新Index的slotValue中，从而形成了一个链表的结构。
 * <p>
 * Index Linked List的位置（absIndexPos）的计算公式： 40+ 500W*4+index的顺序数*40；
 * <p>
 * 当对请求消息生成索引时，就是先计算出absSlotPos和absIndexPos值；然后在按照上面的数据结构将值写入对于的位置即可。
 */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 向index文件中写入索引消息
     * <p>
     * <p>
     * 调用putKey(final String key, final long phyOffset, final long storeTimestamp)方法，该方法中的入参key为topic-key值；phyOffset为物理偏移量。
     * <p>
     * 1）首先根据key的Hash值计算出absSlotPos值；
     * <p>
     * 2）根据absSlotPos值作为index文件的读取开始偏移量读取4个字节的值，即为了避免KEY值的hash冲突，将之前的key值的索引顺序数给冲突了，故先从slot Table中的取当前存储的索引顺序数，若该值小于零或者大于当前的索引总数（IndexHeader的indexCount值）则视为无效，即置为0；否则取出该位置的值，放入当前写入索引消息的Index Linked的slotValue字段中；
     * <p>
     * 3）计算当前存时间距离第一个索引消息落在Broker的时间戳beginTimestamp的差值，放入当前写入索引消息的Index Linked的timeOffset字段中；
     * <p>
     * 4）计算absIndexPos值，然后根据数据结构上值写入Index Linked中；
     * <p>
     * 5）将索引总数写入slot Table的absSlotPos位置；
     * <p>
     * 6）若为第一个索引，则更新IndexHeader的beginTimestamp和beginPhyOffset字段；
     * <p>
     * 7）更新IndexHeader的endTimestamp和endPhyOffset字段；
     * <p>
     * 8）将IndexHeader的hashSlotCount和indexCount字段值加1；
     *
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    /**
     * 以topic-key值从Index中获取在一个时间区间内的物理偏移量列表
     * <p>
     * 调用方法是selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,final long begin, final long end, boolean lock)。
     * <p>
     * 在调用该方法之前，先要检查开始时间、结束时间是否落在该Index文件中，调用isTimeMatched(long begin,long end)方法。在该方法中，用IndexHeader的beginTimestamp和EndTimestamp进行比较，若开始时间begin和结束时间end有一部分落在了Index内，则返回true。
     * <p>
     * 在selectPhyOffset方法中。参数key值是topic-key（消息的key）的值。按如下步骤查找物理偏离量列表：
     * 1）计算key值的hash值；然后除以500W取余，得slotPos值；
     * 2）计算absSlotPos=40+slotPos*4；然后从index中以absSlotPos偏移量读取4个字节的整数值，即为该索引的顺序数index；
     * 3）计算absIndexPos=40+ 500W*4+index的顺序数*40；
     * 4）以absIndexPos为开始偏移量从index中读取后面20个字节的消息单元数据。
     * 5）检查读取到的数据中keyHash值是否等于请求参数key值的hash值，存储时间是否在请求时间范围内，若是在存入物理偏移量列表中；
     * 6）然后用读取数据中的slotValue值重新计算absIndexPos；并重新第4/5/6步的操作。这就是说在此次该key值时，Hash值有冲突，在Index Linked List中形成了链表，该链表是由slotValue值连接各个消息单元的。
     *
     * @param phyOffsets
     * @param key
     * @param maxNum
     * @param begin
     * @param end
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
