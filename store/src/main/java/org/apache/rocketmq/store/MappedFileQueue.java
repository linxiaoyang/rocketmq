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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    private final AllocateMappedFileService allocateMappedFileService;

    private long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 获取在某时间点之后更新的文件
     *
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        /**
         * 遍历MapedFile列表，若遇到文件的更新时间戳大于某时间点timestamp
         * 则返回该MapedFile对象，遍历完之后仍然没有找到则返回列表的最后一
         * 个MapedFile对象
         */
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 清理指定偏移量所在文件之后的文件
     *
     * @param offset
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        /**
         * 遍历MapedFile列表，每个MapedFile对象对应着一个固定大小的文件，
         * 若当文件的起始偏移量fileFromOffset<=offset<=fileFromOffset+fileSize，
         * 则表示指定的位置偏移量offset落在的该文件上，则将对应的MapedFile对象的
         * wrotepostion和commitPosition设置为offset%fileSize，若文件的起始偏移
         * 量fileFromOffset>offset，即是命中的文件之后的文件，则将这些文件删除并且
         * 从MappFileQueue的MapedFile列表中清除掉。
         */
        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 统计内存的数据还有多少未持久化
     * <p>
     * <p>
     * 调用getLastMapedFile方法获取Mapped队列中最后一个MapedFile对象，计算得出未刷盘的消息大小，
     * 计算公式为：最后一个MapedFile对象fileFromOffset+写入位置wrotepostion-commitedWhere
     * （上次刷盘的位置）
     *
     * @return
     */
    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取或创建最后一个文件
     *
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        /**
         * 这个offeset是所有MappedFile的全局offset
         */
        long createOffset = -1;
        /**
         * 从MapedFile列表中获取最后一个MapedFile对象，若列表为空或者最后一个对象对应的文件已经写满，
         * 则创建一个新的文件（即新的MapedFile对象）；
         * 若存在最后一个文件（对应最后一个MapedFile对象）并且未写满，则直接返回最后一个MapedFile对象
         */
        MappedFile mappedFileLast = getLastMappedFile();

        /**
         * 设置开始的偏移量
         */
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        /**
         * 若最后一个文件写满，则新文件的文件名等于最后一个文件的fileFromOffset+fileSize；
         */
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            /**
             * 拼接下一个文件的路径
             */
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            /**
             * 下一个下一个的文件的路径
             */
            String nextNextFilePath = this.storePath + File.separator
                    + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;


            /**
             * 若在Broker启动初始化的时候会创建了后台服务线程（AllocateMapedFileService服务），
             * 则调用AllocateMapedFileService.putRequestAndReturnMapedFile方法，在该方法
             * 中用下一个文件的文件路径、下下一个文件的路径、文件大小为参数初始化AllocateRequest
             * 对象，并放入该服务线程的requestQueue:PriorityBlockingQueue<AllocateRequest>
             * 变量中，由该线程在后台监听requestQueue队列，若该队列中存在AllocateRequest对象，
             * 则利用该对象的变量值创建MapedFile对象（即在磁盘中生成了对应的物理文件），并存入
             * AllocateRequest对象的MapedFile变量中，并且在下一个新文件之后继续将下下一个新文件
             * 也创建了。若是在当前线程中直接创建MappeFile对象，则只创建一个新的文件。最后将创建或
             * 返回的MapedFile对象存入MapedFileQueue的MapedFile列表中，并返回该MapedFile对象
             * 给调用者
             */
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                        nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 从MapedFile列表中获取最后一个MapedFile对象，若列表为空或者最后一个对象对应的文件已经写满，
     * 则创建一个新的文件（即新的MapedFile对象）；
     * 若存在最后一个文件（对应最后一个MapedFile对象）并且未写满，则直接返回最后一个MapedFile对象。
     *
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取MapedFile队列中最小Offset值
     * <p>
     * <p>
     * 先获取MapedFile队列中第一个MapedFile对象，再取该对象的fileFromOffset值（即文件的名字值），
     * 该值即为最小offset值；若MapedFile列表为空，则直接返回-1；
     *
     * @return
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取MapedFile队列中最大Offset值
     * <p>
     * 获取Mapped队列中最后一个MapedFile对象，将最后一个MapedFile对象fileFromOffset加上写入位置wrotepostion值即为最大offset值。
     *
     * @return
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }


    /**
     * 删除某类文件中的最后一个文件
     * <p>
     * 例如commit类型的文件下面有多个固定大小（1G）的文件，即对应在MapedFile列表中有多个MapedFile对象，若要删除最后一个文件，
     * 首先从磁盘中删除物理文件，然后从列表中删除最后一个MapedFile对象。
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        for (int i = 0; i < mfsLength; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
            if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                if (mappedFile.destroy(intervalForcibly)) {
                    files.add(mappedFile);
                    deleteCount++;

                    if (files.size() >= DELETE_FILES_BATCH_MAX) {
                        break;
                    }

                    if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                        try {
                            Thread.sleep(deleteFilesInterval);
                        } catch (InterruptedException e) {
                        }
                    }
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, false);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }


    /**
     * MappedFile队列中的消息刷盘
     * <p>
     * <p>
     * 首先根据commitWhere值（上次刷盘的位置）在队列中找到所处的文件对象MapedFile，
     * 即调用findMapedFileByOffset方法；然后调用MapedFile对象的commit方法完成消
     * 息刷盘操作；最后利用MapedFile对象的fileFromOffset值加上这次刷盘的消息大小得
     * 到的总和更新commitWhere值，作为下次刷盘的开始位置，同时更新MapedFileQueue对
     * 象的存储时间戳（storeTimestamp）
     *
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, false);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     * 根据指定的offset找到所在文件
     * <p>
     * 方法findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound)：
     * 首先找到MapedFile队列中的第一个MapedFile对象，取该对象的fileFromOffset值；
     * 然后指定的offset减去fileFromOffset值再除以fileSize得到文件在MapedFile队列中的序列号index，
     * 最后根据index值从列表中获取MapedFile对象。若index值获取对象时出错误（index<0或者大于列表的总数），
     * 则根据returnFirstOnNotFound参数决定是返回null或者第一个MapedFile对象。
     *
     * @param offset                Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile mappedFile = this.getFirstMappedFile();
            if (mappedFile != null) {
                int index = (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                if (index < 0 || index >= this.mappedFiles.size()) {
                    LOG_ERROR.warn("Offset for {} not matched. Request offset: {}, index: {}, " +
                                    "mappedFileSize: {}, mappedFiles count: {}",
                            mappedFile,
                            offset,
                            index,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                    LOG_ERROR.warn("findMappedFileByOffset failure. ", e);
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
