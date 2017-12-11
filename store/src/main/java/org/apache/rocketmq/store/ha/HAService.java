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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 消息数据同步
 * 在主备Broker启动的时候，启动HAService服务，启动了如下服务
 */
public class HAService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
                new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
                result
                        && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                        .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * <p>
     * 该服务是主用Broker使用，该服务主要监听新的Socket连接，若有新的连接到来，
     * 则创建HAConnection对象，在该对象中创建了HAConnection.WriteSocketService
     * 和HAConnection.ReadSocketService线程服务，对新来的socket连接分别进行读和写的监听
     * <p>
     * <p>
     * <p>
     * 在启动该模块时，注册监听Socket的OP_READ操作，若有该操作达到，即有备用Broker请求连接到
     * 该主用Broker上时，利用该ScoketChannel初始化HAConnection对象，并调用该对象的start方法，
     * 在该方法中启动该对象的ReadSocketService和WriteSocketService服务线程，然后将该
     * HAConnection对象存入HAService.connectionList:List<HAConnection>变量中，该变量是存
     * 储客户端连接，用于管理连接的删除和销毁。
     * <p>
     * ReadSocketService服务线程用于读取备用Broker的请求，WriteSocketService服务线程用于向备用Broker写入数据。
     */
    class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                            + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * <p>
     * <p>
     * 该服务是对同步进度进行监听，若达到应用层的写入偏移量，则通知应用层该同步已经完成。
     * 在调用CommitLog.putMessage方法写入消息内容时，根据主用broker的配置来决定是否
     * 利用该服务进行同步等待数据同步的结果
     * <p>
     * <p>
     * 在该模块中有两个请求队列分别是GroupTransferService.requestsRead：List<GroupCommitRequest>
     * 和requestsWrite:List<GroupCommitRequest>,应用层将请求放入requestsWrite队列中，
     * 当该服务线程被唤醒时，首先将requestsWrite队列的请求与requestsRead队列的请求交换，
     * 而requestsRead队列一般都是空的，也就是将requestsWrite队列的内容赋值到requestsRead队列之后再清空
     * ；然后遍历requestsRead队列的每个GroupCommitRequest对象，用该对象的
     * NextOffset值与push2SlaveMaxOffset比较，若push2SlaveMaxOffset大于
     * 了该对象的NextOffset值则置GroupCommitRequest请求对象的flushOK变量为true，
     * 在调用处对该变量有监听。
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        for (int i = 0; !transferOK && i < 5; i++) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }


    /**
     * 该服务是备用Broker使用，在备用Broker启动之后与主用Broker建立socket连接，
     * 然后将备用Broker的commitlog文件的最大数据位置每隔5秒给主用Broker发送一次；
     * 并监听主用Broker的返回消息。
     * <p>
     * <p>
     * <p>
     * 在备用Broker启动的时候，启动了HAService.HAClient线程服务，该线程有两个作用，第一，每隔5秒发送一次心跳消息；第二，接受主用Broker的返回数据，然后进行后续处理。在该服务未停止的情况下，循环的执行下列步骤。
     * <p>
     * 1、从HAService.HAClient.masterAddress变量中获取主用Broker的地址，在备用Broker向Name Server注册时会返回主用Broker的地址；若有主用Broker地址则与主用Broker建立Socket链接，并在该链接上注册OP_READ操作，即监听主用Broker返回的消息；调用DefaultMessageStore.getMaxPhyOffset()方法获取备用Broker本地的最大写入位置即最大物理偏移量，然后赋值给HAClient.currentReportedOffset变量；更新最后写入时间戳lastWriteTimestamp；
     * <p>
     * 2、检查上次写入时间戳lastWriteTimestamp距离现在是否已经过了5秒，即每隔5秒向主用Broker进行一次物理偏移量报告（HAClient.currentReportedOffset）；若超过了5秒，则备用Broker向主用Broker报告备用Broker的当前最大物理偏移量的值，该消息只有8个字节，即为物理偏移量的值；
     * <p>
     * 3、该服务线程等待1秒钟，然后从SocketChannel读取主用Broker返回的数据，一直循环的读取并解析数据，直到HAClient.byteBufferRead:ByteBuffer中无可写的空间为止，当ByteBuffer中的this.position<this.limit时表示有可写空间,该byteBufferRead变量是在初始化时HAClient是创建的，初始化空间为ReadMaxBufferSize=4G，若为空将重复读取3次后仍然没有数据则跳出该循环。若读取到数据，则首先更新HAClient.lastWriteTimestamp变量；然后调用HAClient.dispatchReadRequest()方法对数据解析和处理，在dispatchReadRequest方法中循环的读取byteBufferRead变量中的数据，具体处理步骤如下：
     * <p>
     * 3.1）用HAClient.dispatchPostion变量来标记从byteBufferRead变量读取数据的位置；初始化值为0；
     * <p>
     * 3.2）byteBufferRead变量的position值表示从SocketChannel中收到的数据的最后位置；首先将此position值赋值给临时变量readSocketPos，比较position减dispatchPostion的值大于12（消息头部长度为12个字节）：
     * <p>
     * 1）若小于12个字节，并且byteBufferRead变量中没有可写空间（this.position>=this.limit）,则调用HAClient.reallocateByteBuffer()方法进行ByteBuffer的整理，然后返回继续执行第4步；整理过程如下：
     * <p>
     * 1.A）检查byteBufferRead变量中的二进制数据是否解析完了（reamin=ReadMaxBufferSize-dispatchPostion），若reamin>0表示没有解析完，则将剩下的数据复制到HAClient.byteBufferBackup变量中；
     * <p>
     * 1.B）将byteBufferRead和byteBufferBackup的数据进行交换；
     * <p>
     * 1.C）重新初始化byteBufferRead变量的position等于reamin，即表示byteBufferRead中写入到了位置position；
     * <p>
     * 2）若大于12个字节表示有心跳消息从主用Broker发送过来，进行如下处理：
     * <p>
     * 2.A）在byteBufferRead中从dispatchPostion位置开始读取数据，初始化状态下dispatchPostion等于0；读取8个字节的数据即为主用Broker的同步的起始物理偏移量masterPhyOffset，再后4字节为数据的大小bodySize；
     * <p>
     * 2.B）从备用Broker中获取最大的物理偏移量，若与主用Broker传来的起始物理偏移量masterPhyOffset不相等，则直接返回继续执行第4步；
     * <p>
     * 2.C）若position-dispatchPostion的值大于消息头部长度12字节加上bodySize之和；则说明有数据同步，则继续在byteBufferRead中以position+dispatchPostion开始位置读取bodySize大小的数据；
     * <p>
     * 2.D）调用DefaultMessageStore.appendToCommitLog(long startOffset, byte[] data)方法进行数据的写入；
     * <p>
     * 2.E）将byteBufferRead变量的position值重置为readSocketPos；dispatchPostion值累计12+bodySize；
     * <p>
     * 2.F）检查当前备用Broker的最大物理偏移量是否大于了上次向主用Broker报告时的最大物理偏移量（HAClient.currentReportedOffset），若大于则更新HAClient.currentReportedOffset的值，并将最新的物理偏移量向主用Broker报告。
     * <p>
     * 2.G）继续读取byteBufferRead变量中的数据并执行第3.1和3.2步；
     * <p>
     * 4、在第3步返回之后，检查当前备用Broker的最大物理偏移量是否大于了上次向主用Broker报告时的最大物理偏移量（HAClient.currentReportedOffset），若大于则更新HAClient.currentReportedOffset的值，并将最新的物理偏移量向主用Broker报告。
     * <p>
     * 5、检查最后写入时间戳lastWriteTimestamp距离当前的时间，若大于了5秒，表示这期间未收到过主用Broker的消息，则关闭与主用Broker的连接；
     * <p>
     * Master-Slave方式就遇到了主从复制延迟的问题（异步复制永远是延迟的），那么在Master不可用后可能会导致部分数据丢失。针对这种场景，提供了同步双写的模式。
     * <p>
     * 主备Broker的异步同步和同步双写在同步流程上面没有区别，只是在写入消息时，同步双写会创建一个GroupCommitRequest请求，由GroupTransferService服务线程来监听同步的进度是否达到了该请求的要求，若达到了则返回通知调用者；而异步同步时调用者不关心同步的进度，同步完成之后也不需通知任何调用者
     */
    class HAClient extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;
        private long lastWriteTimestamp = System.currentTimeMillis();

        private long currentReportedOffset = 0;
        private int dispatchPostion = 0;
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                    HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                    .getHaSendHeartbeatInterval();

            return needHeart;
        }

        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                            + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                if (diff >= msgHeaderSize) {
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                    + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    if (this.connectMaster()) {

                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);

                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                                HAService.this.getDefaultMessageStore().getSystemClock().now()
                                        - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                                .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                    + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
