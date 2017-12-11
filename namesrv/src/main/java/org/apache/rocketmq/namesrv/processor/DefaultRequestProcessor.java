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
package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteTopicInNamesrvRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        if (log.isDebugEnabled()) {
            log.debug("receive request, {} {} {}",
                    request.getCode(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);
            case RequestCode.REGISTER_BROKER:
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request);
                } else {
                    return this.registerBroker(ctx, request);
                }
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            case RequestCode.GET_ROUTEINTO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return getAllTopicListFromNameserver(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return deleteTopicInNamesrv(ctx, request);
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, request);
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request);
            case RequestCode.UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, request);
            case RequestCode.GET_NAMESRV_CONFIG:
                return this.getConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand putKVConfig(ChannelHandlerContext ctx,
                                       RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
                (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().putKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey(),
                requestHeader.getValue()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand getKVConfig(ChannelHandlerContext ctx,
                                       RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
                (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: " + requestHeader.getKey());
        return response;
    }

    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), RegisterBrokerBody.class);
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                registerBrokerBody.getTopicConfigSerializeWrapper(),
                registerBrokerBody.getFilterServerList(),
                ctx.channel());

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 处理Broker注册请求
     * <p>
     * 在接收到REGISTER_BROKER请求码之后，由DefaultRequestProcessor.registerBroker(ChannelHandlerContext ctx, RemotingCommand request)方法处理注册请求，在该方法中调用 RouteInfoManager.registerBroker(String clusterName, String brokerAddr, String brokerName, long brokerId, String haServerAddr, TopicConfigSerializeWrapper topicConfigWrapper, List<String> filterServerList, Channel channel)方法完成注册处理逻辑，注册完成后返回给Broker端主用Broker的地址，以及该主用Broker的HA服务地址。
     * <p>
     * 1、维护RouteInfoManager.clusterAddrTable变量；若Broker集群名字不在该Map变量中，则初始化一个Set集合，将brokerName存入该Set集合中，然后以clusterName为key值，该Set集合为values值存入此Map变量中；说明一个集群下面有多个Broker；
     * <p>
     * 2、维护RouteInfoManager.brokerAddrTable变量，该变量是维护Broker的名称、ID、地址等信息的。若该brokername不在该Map变量中，则创建BrokerData对象，该对象包含了brokername，以及brokerId和brokerAddr为K-V的brokerAddrs变量；然后以brokername为key值将BrokerData对象存入该brokerAddrTable变量中；说明同一个BrokerName下面可以有多个不同BrokerId的Broker存在，表示一个BrokerName有多个Broker存在，通过BrokerId来区分主备。
     * <p>
     * 3、若Broker的注册请求消息中topic的配置不为空，并且该Broker是主用（即brokerId=0），则根据NameServer存储的Broker版本信息来判断是否需要更新NameServer端的topic配置信息。若是第一个注册，即表示Name Server端还没有该Broker的topic信息，或者在NameServer中维护的该Broker的dataversion与收到的topic配置（TopicConfigSerializeWrapper）的dataversion不一致；则更新NameServer端的topic配置信息。大致处理思路：
     * <p>
     * 遍历TopicConfigSerializeWrapper对象的topicConfigTable变量，以该变量中的每个TopicConfig对象以及该Broker的名称为参数调用RouteInfoManager.createAndUpdateQueueData(String brokerName, TopicConfig topicConfig) 方法，该方法的逻辑是：
     * <p>
     * A）用TopicConfig对象中的各变量来初始化QueueData对象，包括的变量有：brokerName、读写队列数、权限等信息；
     * <p>
     * B）维护RouteInfoManager.topicQueueTable变量，该变量是维护topic配置信息的。以topic的名字从RouteInfoManager.topicQueueTable变量中获取QueueData列表；若没有则初始化该QueueData队列并将上一步初始化的QueueData对象存入；若已经存在QueueData队列，则遍历该队列中的每个QueueData对象，检查是否与该注册的Broker相同的名字的QueueData对象：
     * <p>
     * 若相同则检查该QueueData对象的各个属性值是否与新创建的QueueData对象（在第A步中创建的）一致，若一致则表示是topic的配置没有变化则不更新QueueData队列的该QueueData对象，若不一致，则认为该topic有变化，要将该QueueData对象从QueueData列表中删除，然后将新创建的QueueData对象加入到此队列中；
     * <p>
     * 若不相同则直接将QueueData对象加入到此QueueData队列中；
     * <p>
     * 4、初始化BrokerLiveInfo 对象并以broker地址为key值存入brokerLiveTable:HashMap<String/* brokerAddr , BrokerLiveInfo>变量中；
     * <p>
     * 5、对于filterServerList不为空的，以broker地址为key值存入RouteInfoManager.filterServerTable :HashMap<String/* brokerAddr , List<String>/* Filter Server >变量中；
     * <p>
     * 6、找到该BrokerName下面的主用Broker（BrokerId=0），方法是以该Brokerame从RouteInfoManager.BrokerAddrTable变量中获取BrokerData对象，然后取该对象的brokerAddrs变量，从该变量中找到BrokerId=0的Broker地址，作为该注册Broker对应的主用地址MasterAddr； 以主用Broker地址从brokerLiveTable中获取BrokerLiveInfo 对象，取该对象的HaServerAddr 值，将该地址作为HaServerAddr返回给Broker端，即主用Broker的HaServerAddr为其他备用Broker的HAClient.masterAddress值。
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        TopicConfigSerializeWrapper topicConfigWrapper;
        if (request.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                topicConfigWrapper,
                null,
                ctx.channel()
        );

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx,
                                            RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader =
                (UnRegisterBrokerRequestHeader) request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        this.namesrvController.getRouteInfoManager().unregisterBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 根据Topic获取Broker信息和topic配置信息
     * <p>
     * 当NameServer收到GET_ROUTEINTO_BY_TOPIC 请求码之后，间接调用了RouteInfoManager.pickupTopicRouteData(String topic)方法获取Broker信息和topic配置信息。
     * <p>
     * 1、获取topic配置信息，根据topic从RouteInfoManager.topicQueueTable变量中获取List<QueueData>队列，赋值给返回对象TopicRouteData的QueueDatas变量。表示该topic对应的所有topic配置信息以及每个配置所属的BrokerName。
     * <p>
     * 2、从上一步获取到的List<QueueData>队列中获取BrokerName集合，该集合是去重之后的BrokerName集合，然后以该BrokerName集合的每个BrokerName从RouteInfoManager.brokerAddrTable变量中获取BrokerData对象，将所有获取到的BrokerData对象集合赋值给返回对象TopicRouteData的BrokerDatas集合变量。表示该topic是由哪些Broker提供的服务，以及每个Broker的名字、BrokerId、IP地址。
     * <p>
     * 然后以"ORDER_TOPIC_CONFIG"和请求消息中的topic值为参数从NamesrvController.kvConfigManager.configTable: HashMap<String/* Namespace , HashMap<String/* Key , String/* Value >>变量中获取orderTopiconf值（即broker的顺序），并赋值给TopicRouteData.orderTopicConf变量；该orderTopiconf的参数格式为：以";"解析成数组，数组的每个成员是以":"分隔的，构成数据"brokerName:queueNum"；
     * <p>
     * 最后将TopicRouteData对象返回给调用者；
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
                (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                        this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                                requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo();
        response.setBody(content);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx,
                                                  RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader =
                (WipeWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());

        log.info("wipe write perm of broker[{}], client: {}, {}",
                requestHeader.getBrokerName(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                wipeTopicCnt);

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicInNamesrvRequestHeader requestHeader =
                (DeleteTopicInNamesrvRequestHeader) request.decodeCommandCustomHeader(DeleteTopicInNamesrvRequestHeader.class);

        this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ctx,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader =
                (GetKVListByNamespaceRequestHeader) request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
                requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }

    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicsByClusterRequestHeader requestHeader =
                (GetTopicsByClusterRequestHeader) request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);

        byte[] body = this.namesrvController.getRouteInfoManager().getTopicsByCluster(requestHeader.getCluster());

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx,
                                                     RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getSystemTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx,
                                             RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getUnitTopics();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }

            if (bodyStr == null) {
                log.error("updateConfig get null body!");
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            this.namesrvController.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}
