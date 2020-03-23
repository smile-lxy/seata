/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.rpc;

import io.netty.channel.Channel;
import io.seata.common.Constants;
import io.seata.common.exception.FrameworkException;
import io.seata.common.util.StringUtils;
import io.seata.core.protocol.IncompatibleVersionException;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.Version;
import io.seata.core.rpc.netty.NettyPoolKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The type channel manager.
 *
 * @author slievrly
 */
public class ChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);
    private static final ConcurrentMap<Channel, RpcContext> IDENTIFIED_CHANNELS
        = new ConcurrentHashMap<Channel, RpcContext>();

    /**
     * resourceId -> applicationId -> ip -> port -> RpcContext
     */
    private static final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
        RpcContext>>>>
        RM_CHANNELS = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
        RpcContext>>>>();

    /**
     * appname+ip,port
     */
    private static final ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> TM_CHANNELS
        = new ConcurrentHashMap<String, ConcurrentMap<Integer, RpcContext>>();

    /**
     * Is registered boolean.
     *
     * @param channel the channel
     * @return the boolean
     */
    public static boolean isRegistered(Channel channel) {
        return IDENTIFIED_CHANNELS.containsKey(channel);
    }

    /**
     * Gets get role from channel.
     *
     * @param channel the channel
     * @return the get role from channel
     */
    public static NettyPoolKey.TransactionRole getRoleFromChannel(Channel channel) {
        if (IDENTIFIED_CHANNELS.containsKey(channel)) {
            return IDENTIFIED_CHANNELS.get(channel).getClientRole();
        }
        return null;
    }

    /**
     * 根据身份获取RPCContext
     * Gets get context from identified.
     *
     * @param channel the channel
     * @return the get context from identified
     */
    public static RpcContext getContextFromIdentified(Channel channel) {
        return IDENTIFIED_CHANNELS.get(channel);
    }

    private static String buildClientId(String applicationId, Channel channel) {
        return applicationId + Constants.CLIENT_ID_SPLIT_CHAR + ChannelUtil.getAddressFromChannel(channel);
    }

    private static String[] readClientId(String clientId) {
        return clientId.split(Constants.CLIENT_ID_SPLIT_CHAR);
    }

    /**
     * 构建RPC上下文
     */
    private static RpcContext buildChannelHolder(NettyPoolKey.TransactionRole clientRole, String version, String applicationId,
                                                 String txServiceGroup, String dbkeys, Channel channel) {
        RpcContext holder = new RpcContext();
        holder.setClientRole(clientRole);
        holder.setVersion(version);
        holder.setClientId(buildClientId(applicationId, channel));
        holder.setApplicationId(applicationId);
        holder.setTransactionServiceGroup(txServiceGroup);
        holder.addResources(dbKeytoSet(dbkeys));
        holder.setChannel(channel);
        return holder;
    }

    /**
     * Register tm channel.
     *
     * @param request the request
     * @param channel the channel
     * @throws IncompatibleVersionException the incompatible version exception
     */
    public static void registerTMChannel(RegisterTMRequest request, Channel channel)
        throws IncompatibleVersionException {
        // 检查版本
        Version.checkVersion(request.getVersion());
        // 封装RPC上下文
        RpcContext rpcContext = buildChannelHolder(NettyPoolKey.TransactionRole.TMROLE, request.getVersion(),
            request.getApplicationId(), request.getTransactionServiceGroup(), null, channel);
        // 添加到Channel集合
        rpcContext.holdInIdentifiedChannels(IDENTIFIED_CHANNELS);
        String clientIdentified = rpcContext.getApplicationId() + Constants.CLIENT_ID_SPLIT_CHAR
            + ChannelUtil.getClientIpFromChannel(channel);
        TM_CHANNELS.putIfAbsent(clientIdentified, new ConcurrentHashMap<Integer, RpcContext>());
        ConcurrentMap<Integer, RpcContext> clientIdentifiedMap = TM_CHANNELS.get(clientIdentified);
        // 缓存Channel上下文
        rpcContext.holdInClientChannels(clientIdentifiedMap);
    }

    /**
     * 注册RM Channel
     * Register rm channel.
     *
     * @param resourceManagerRequest the resource manager request
     * @param channel                the channel
     * @throws IncompatibleVersionException the incompatible  version exception
     */
    public static void registerRMChannel(RegisterRMRequest resourceManagerRequest, Channel channel)
        throws IncompatibleVersionException {
        // 检查版本
        Version.checkVersion(resourceManagerRequest.getVersion());
        Set<String> dbkeySet = dbKeytoSet(resourceManagerRequest.getResourceIds());
        RpcContext rpcContext;
        if (!IDENTIFIED_CHANNELS.containsKey(channel)) {
            // 缓存中不存在, 封装, 缓存
            rpcContext = buildChannelHolder(NettyPoolKey.TransactionRole.RMROLE, resourceManagerRequest.getVersion(),
                resourceManagerRequest.getApplicationId(), resourceManagerRequest.getTransactionServiceGroup(),
                resourceManagerRequest.getResourceIds(), channel);
            rpcContext.holdInIdentifiedChannels(IDENTIFIED_CHANNELS);
        } else {
            // 更新DB资源地址
            rpcContext = IDENTIFIED_CHANNELS.get(channel);
            rpcContext.addResources(dbkeySet);
        }
        if (null == dbkeySet || dbkeySet.isEmpty()) { return; }
        for (String resourceId : dbkeySet) {
            String clientIp;
            ConcurrentMap<Integer, RpcContext> portMap = RM_CHANNELS.computeIfAbsent(resourceId, resourceIdKey -> new ConcurrentHashMap<>())
                //  ^ ApplicationID, IP, Port, RpcContext
                    .computeIfAbsent(resourceManagerRequest.getApplicationId(), applicationId -> new ConcurrentHashMap<>())
                //  ^ IP, Port, RpcContext
                    .computeIfAbsent(clientIp = ChannelUtil.getClientIpFromChannel(channel), clientIpKey -> new ConcurrentHashMap<>());
                //  ^ Port, RpcContext
            // 缓存
            rpcContext.holdInResourceManagerChannels(resourceId, portMap);
            updateChannelsResource(resourceId, clientIp, resourceManagerRequest.getApplicationId());
        }

    }

    /**
     * 更新Channel资源
     */
    private static void updateChannelsResource(String resourceId, String clientIp, String applicationId) {
        ConcurrentMap<Integer, RpcContext> sourcePortMap = RM_CHANNELS.get(resourceId).get(applicationId).get(clientIp);
        for (ConcurrentMap.Entry<String, ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>>>> rmChannelEntry
            : RM_CHANNELS.entrySet()) {
            if (rmChannelEntry.getKey().equals(resourceId)) {
                // 如果相同, 跳过
                continue;
            }
            // ApplicationId, IP, Port, RPCContext
            ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>>> applicationIdMap = rmChannelEntry.getValue();
            if (!applicationIdMap.containsKey(applicationId)) {
                // 不包含该ApplicationID, 跳过
                continue;
            }
            // IP, Port, RPCContext
            ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> clientIpMap = applicationIdMap.get(applicationId);
            if (!clientIpMap.containsKey(clientIp)) {
                // 不包含该IP, 跳过
                continue;
            }
            // Port, RPCContext
            ConcurrentMap<Integer, RpcContext> portMap = clientIpMap.get(clientIp);
            for (ConcurrentMap.Entry<Integer, RpcContext> portMapEntry : portMap.entrySet()) {
                Integer port = portMapEntry.getKey();
                if (!sourcePortMap.containsKey(port)) {
                    // 不包含该Port, 缓存
                    RpcContext rpcContext = portMapEntry.getValue();
                    sourcePortMap.put(port, rpcContext);
                    rpcContext.holdInResourceManagerChannels(resourceId, port);
                }
            }
        }
    }

    /**
     * ','分隔去重
     * @param dbkey
     * @return
     */
    private static Set<String> dbKeytoSet(String dbkey) {
        if (StringUtils.isNullOrEmpty(dbkey)) {
            return null;
        }
        return new HashSet<String>(Arrays.asList(dbkey.split(Constants.DBKEYS_SPLIT_CHAR)));
    }

    /**
     * Release rpc context.
     *
     * @param channel the channel
     */
    public static void releaseRpcContext(Channel channel) {
        if (IDENTIFIED_CHANNELS.containsKey(channel)) {
            RpcContext rpcContext = getContextFromIdentified(channel);
            rpcContext.release();
        }
    }

    /**
     * 获取相同业务的Channel
     * Gets get same income client channel.
     *
     * @param channel the channel
     * @return the get same income client channel
     */
    public static Channel getSameClientChannel(Channel channel) {
        if (channel.isActive()) {
            // 如果当前Channel存活, 返回
            return channel;
        }
        // 根据身份获取上下文
        RpcContext rpcContext = getContextFromIdentified(channel);
        if (null == rpcContext) {
            LOGGER.error("rpcContext is null,channel:{},active:{}", channel, channel.isActive());
            return null;
        }
        if (rpcContext.getChannel().isActive()) {
            // Channel是存活的, 返回
            return rpcContext.getChannel();
        }
        Integer clientPort = ChannelUtil.getClientPortFromChannel(channel);
        // Channel 身份
        NettyPoolKey.TransactionRole clientRole = rpcContext.getClientRole();
        // 根据不同身份, 从各自缓存中查找
        if (clientRole == NettyPoolKey.TransactionRole.TMROLE) {
            // TM
            String clientIdentified = rpcContext.getApplicationId() + Constants.CLIENT_ID_SPLIT_CHAR
                + ChannelUtil.getClientIpFromChannel(channel);
            if (!TM_CHANNELS.containsKey(clientIdentified)) {
                // 不存在指定身份ID的上下文, 直接返回
                return null;
            }
            ConcurrentMap<Integer, RpcContext> clientRpcMap = TM_CHANNELS.get(clientIdentified);

            return getChannelFromSameClientMap(clientRpcMap, clientPort);
        } else if (clientRole == NettyPoolKey.TransactionRole.RMROLE) {
            // RM
            for (Map<Integer, RpcContext> clientRmMap : rpcContext.getClientRMHolderMap().values()) {
                Channel sameClientChannel = getChannelFromSameClientMap(clientRmMap, clientPort);
                if (null != sameClientChannel) {
                    return sameClientChannel;
                }
            }
        }
        return null;

    }

    /**
     * 根据Port获取存活的Channel(移除失效的Channel)
     */
    private static Channel getChannelFromSameClientMap(Map<Integer, RpcContext> clientChannelMap, int exclusivePort) {
        if (null != clientChannelMap && !clientChannelMap.isEmpty()) {
            for (ConcurrentMap.Entry<Integer, RpcContext> entry : clientChannelMap.entrySet()) {
                if (entry.getKey() == exclusivePort) {
                    // 缓存中移除
                    clientChannelMap.remove(entry.getKey());
                    continue;
                }
                Channel channel = entry.getValue().getChannel();
                if (channel.isActive()) {
                    // Channel是存活的, 返回
                    return channel;
                }
                // 缓存中移除
                clientChannelMap.remove(entry.getKey());
            }
        }
        return null;
    }

    /**
     * Gets get channel.
     *
     * @param resourceId Resource ID
     * @param clientId   Client ID - ApplicationId:IP:Port
     * @return Corresponding channel, NULL if not found.
     */
    public static Channel getChannel(String resourceId, String clientId) {
        Channel resultChannel = null;

        String[] clientIdInfo = readClientId(clientId);

        if (clientIdInfo == null || clientIdInfo.length != 3) {
            throw new FrameworkException("Invalid Client ID: " + clientId);
        }

        String targetApplicationId = clientIdInfo[0];
        String targetIP = clientIdInfo[1];
        int targetPort = Integer.parseInt(clientIdInfo[2]);

        // 资源对应的applicationID, ip, port, 上下文信息
        ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
            RpcContext>>> applicationIdMap = RM_CHANNELS.get(resourceId);

        if (targetApplicationId == null || applicationIdMap == null ||  applicationIdMap.isEmpty()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("No channel is available for resource[{}]", resourceId);
            }
            return null;
        }

        // ip, port, RPCContext
        ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> ipMap = applicationIdMap.get(targetApplicationId);

        if (null != ipMap && !ipMap.isEmpty()) {
            // port, RPCContext
            // Firstly, try to find the original channel through which the branch was registered.
            ConcurrentMap<Integer, RpcContext> portMapOnTargetIP = ipMap.get(targetIP);
            if (portMapOnTargetIP != null && !portMapOnTargetIP.isEmpty()) {

                // RPC上下文
                RpcContext exactRpcContext = portMapOnTargetIP.get(targetPort);
                if (exactRpcContext != null) {
                    Channel channel = exactRpcContext.getChannel();
                    if (channel.isActive()) {
                        // Channel是存活的
                        resultChannel = channel;
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Just got exactly the one {} for {}", channel, clientId);
                        }
                    } else {
                        // 缓存中移除
                        if (portMapOnTargetIP.remove(targetPort, exactRpcContext)) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Removed inactive {}", channel);
                            }
                        }
                    }
                }

                // 尝试相同Port其他的Channel
                // The original channel was broken, try another one.
                if (resultChannel == null) {
                    for (ConcurrentMap.Entry<Integer, RpcContext> portMapOnTargetIPEntry : portMapOnTargetIP.entrySet()) {
                        Channel channel = portMapOnTargetIPEntry.getValue().getChannel();

                        if (channel.isActive()) {
                            resultChannel = channel;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(
                                    "Choose {} on the same IP[{}] as alternative of {}", channel, targetIP, clientId);
                            }
                            break;
                        } else {
                            // 缓存中移除
                            if (portMapOnTargetIP.remove(portMapOnTargetIPEntry.getKey(), portMapOnTargetIPEntry.getValue())) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Removed inactive {}", channel);
                                }
                            }
                        }
                    }
                }
            }

            // 尝试其他节点上的Channel
            // No channel on the this app node, try another one.
            if (resultChannel == null) {
                for (ConcurrentMap.Entry<String, ConcurrentMap<Integer, RpcContext>> ipMapEntry : ipMap.entrySet()) {
                    if (ipMapEntry.getKey().equals(targetIP)) { continue; }

                    ConcurrentMap<Integer, RpcContext> portMapOnOtherIP = ipMapEntry.getValue();
                    if (portMapOnOtherIP == null || portMapOnOtherIP.isEmpty()) {
                        continue;
                    }

                    for (ConcurrentMap.Entry<Integer, RpcContext> portMapOnOtherIPEntry : portMapOnOtherIP.entrySet()) {
                        Channel channel = portMapOnOtherIPEntry.getValue().getChannel();

                        if (channel.isActive()) {
                            resultChannel = channel;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Choose {} on the same application[{}] as alternative of {}", channel, targetApplicationId, clientId);
                            }
                            break;
                        } else {
                            if (portMapOnOtherIP.remove(portMapOnOtherIPEntry.getKey(),
                                portMapOnOtherIPEntry.getValue())) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Removed inactive {}", channel);
                                }
                            }
                        }
                    }
                    if (resultChannel != null) { break; }
                }
            }
        }

        // 尝试相同Resource其他的Application
        if (resultChannel == null) {
            resultChannel = tryOtherApp(applicationIdMap, targetApplicationId);

            if (resultChannel == null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("No channel is available for resource[{}] as alternative of {}", resourceId, clientId);
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Choose {} on the same resource[{}] as alternative of {}", resultChannel, resourceId, clientId);
                }
            }
        }

        return resultChannel;

    }

    /**
     * 获取其他Resource上的Channel
     */
    private static Channel tryOtherApp(ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
        RpcContext>>> applicationIdMap, String myApplicationId) {
        Channel chosenChannel = null;
        for (ConcurrentMap.Entry<String, ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>>> applicationIdMapEntry
            : applicationIdMap.entrySet()) {
            if (!StringUtils.isNullOrEmpty(myApplicationId) && applicationIdMapEntry.getKey().equals(myApplicationId)) {
                // 其他Resource上没有对应的ApplicationID, 跳过
                continue;
            }

            ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> targetIPMap = applicationIdMapEntry.getValue();
            if (targetIPMap == null || targetIPMap.isEmpty()) {
                // 数据为空, 跳过
                continue;
            }

            for (ConcurrentMap.Entry<String, ConcurrentMap<Integer, RpcContext>> targetIPMapEntry
                : targetIPMap.entrySet()) {
                ConcurrentMap<Integer, RpcContext> portMap = targetIPMapEntry.getValue();
                if (portMap == null || portMap.isEmpty()) {
                    // 数据为空, 跳过
                    continue;
                }

                for (ConcurrentMap.Entry<Integer, RpcContext> portMapEntry : portMap.entrySet()) {
                    Channel channel = portMapEntry.getValue().getChannel();
                    if (channel.isActive()) {
                        chosenChannel = channel;
                        break;
                    } else {
                        // 缓存中移除
                        if (portMap.remove(portMapEntry.getKey(), portMapEntry.getValue())) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Removed inactive {}", channel);
                            }
                        }
                    }
                }
                if (chosenChannel != null) { break; }
            }
            if (chosenChannel != null) { break; }
        }
        return chosenChannel;

    }

    /**
     * get rm channels
     *
     * @return
     */
    public static Map<String,Channel> getRmChannels() {
        if (RM_CHANNELS.isEmpty()) {
            return null;
        }
        Map<String,Channel> channels = new HashMap<>(RM_CHANNELS.size());
        for (String resourceId : RM_CHANNELS.keySet()) {
            Channel channel = tryOtherApp(RM_CHANNELS.get(resourceId), null);
            if (channel == null) {
                continue;
            }
            channels.put(resourceId, channel);
        }
        return channels;
    }
}
