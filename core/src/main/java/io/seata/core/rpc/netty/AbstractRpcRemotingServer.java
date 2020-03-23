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
package io.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.RpcContext;
import io.seata.core.rpc.ServerMessageListener;
import io.seata.core.rpc.ServerMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * The type Rpc remoting server.
 *
 * @author slievrly
 * @author xingfudeshi@gmail.com
 */
public abstract class AbstractRpcRemotingServer extends AbstractRpcRemoting implements ServerMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingServer.class);

    private final RpcServerBootstrap serverBootstrap;

    /**
     * The Server message listener.
     */
    private ServerMessageListener serverMessageListener;

    private TransactionMessageHandler transactionMessageHandler;

    private RegisterCheckAuthHandler checkAuthHandler;

    /**
     * Instantiates a new Rpc remoting server.
     *
     * @param messageExecutor   the message executor
     * @param nettyServerConfig the netty server config
     */
    public AbstractRpcRemotingServer(final ThreadPoolExecutor messageExecutor, NettyServerConfig nettyServerConfig) {
        super(messageExecutor);
        // 实例化 Bootstrap
        serverBootstrap = new RpcServerBootstrap(nettyServerConfig);
    }

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler) {
        setHandler(transactionMessageHandler, null);
    }

    private void setHandler(TransactionMessageHandler transactionMessageHandler, RegisterCheckAuthHandler checkAuthHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
        this.checkAuthHandler = checkAuthHandler;
    }

    public TransactionMessageHandler getTransactionMessageHandler() {
        return transactionMessageHandler;
    }

    public RegisterCheckAuthHandler getCheckAuthHandler() {
        return checkAuthHandler;
    }

    /**
     * Sets server message listener.
     *
     * @param serverMessageListener the server message listener
     */
    public void setServerMessageListener(ServerMessageListener serverMessageListener) {
        this.serverMessageListener = serverMessageListener;
    }

    /**
     * Gets server message listener.
     *
     * @return the server message listener
     */
    public ServerMessageListener getServerMessageListener() {
        return serverMessageListener;
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    public void setChannelHandlers(ChannelHandler... handlers) {
        serverBootstrap.setChannelHandlers(handlers);
    }

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {
        serverBootstrap.setListenPort(listenPort);
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return serverBootstrap.getListenPort();
    }

    @Override
    public void init() {
        // netty业务Request超时处理任务
        super.init();
        // 配置netty ServerBootstrap, 启动
        serverBootstrap.start();
    }

    @Override
    public void destroy() {
        serverBootstrap.shutdown();
        super.destroy();
    }

    /**
     * Debug log.
     *
     * @param info the info
     */
    public void debugLog(String info) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(info);
        }
    }

    /**
     * 关闭Channel
     * @param ctx
     */
    private void closeChannelHandlerContext(ChannelHandlerContext ctx) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("closeChannelHandlerContext channel:" + ctx.channel());
        }
        ctx.disconnect();
        ctx.close();
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:{},address:{}", channel, serverAddress);
        }
        channel.disconnect();
        channel.close();
    }

    /**
     * The type ServerHandler.
     */
    @ChannelHandler.Sharable // 在整个生命周期都是以 单例 的形式存在, 会被多个Channel的Pipeline共享, 因此不是线程安全的
    class ServerHandler extends AbstractHandler {

        /**
         * Dispatch.
         *
         * @param request the request
         * @param ctx     the ctx
         */
        @Override
        public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
            Object msg = request.getBody();
            if (msg instanceof RegisterRMRequest) { // RM注册消息
                serverMessageListener.onRegRmMessage(request, ctx, checkAuthHandler);
            } else {
                if (ChannelManager.isRegistered(ctx.channel())) {
                    // 已注册的Channel
                    serverMessageListener.onTrxMessage(request, ctx);
                } else {
                    // 未注册的Channel
                    try {
                        // 关闭Channel
                        closeChannelHandlerContext(ctx);
                    } catch (Exception exx) {
                        LOGGER.error(exx.getMessage());
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(String.format("close a unhandled connection! [%s]", ctx.channel().toString()));
                    }
                }
            }
        }

        /**
         * Channel read.
         *
         * @param ctx the ctx
         * @param msg the msg
         * @throws Exception the exception
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof RpcMessage) {
                // 只处理指定协议消息
                RpcMessage rpcMessage = (RpcMessage) msg;
                debugLog("read:" + rpcMessage.getBody());
                if (rpcMessage.getBody() instanceof RegisterTMRequest) {
                    // 处理TM注册Request
                    serverMessageListener.onRegTmMessage(rpcMessage, ctx, checkAuthHandler);
                    return;
                }
                if (rpcMessage.getBody() == HeartbeatMessage.PING) {
                    // 处理心跳包Request
                    serverMessageListener.onCheckMessage(rpcMessage, ctx);
                    return;
                }
            }
            // 交由父类处理
            super.channelRead(ctx, msg);
        }

        /**
         * 通道不活动时触发
         * Channel inactive.
         *
         * @param ctx the ctx
         * @throws Exception the exception
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            debugLog("inactive:" + ctx);
            if (messageExecutor.isShutdown()) {
                // 线程池已关闭, 不再处理
                return;
            }
            handleDisconnect(ctx);
            super.channelInactive(ctx);
        }

        /**
         * 相对于{@link ChannelManager#releaseRpcContext(Channel)} 少了判断, 但多了次判断(null != rpcContext.getClientRole), 待优化
         * @param ctx
         */
        private void handleDisconnect(ChannelHandlerContext ctx) {
            final String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel()); // 上下文
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(ipAndPort + " to server channel inactive.");
            }
            if (null != rpcContext && null != rpcContext.getClientRole()) {
                rpcContext.release(); // 上下文释放
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove channel:" + ctx.channel() + "context:" + rpcContext);
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove unused channel:" + ctx.channel());
                }
            }
        }

        /**
         * Exception caught.
         *
         * @param ctx   the ctx
         * @param cause the cause
         * @throws Exception the exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("channel exx:" + cause.getMessage() + ",channel:" + ctx.channel());
            }
            // 释放上下文
            ChannelManager.releaseRpcContext(ctx.channel());
            super.exceptionCaught(ctx, cause);
        }

        /**
         * 用户心跳包超时时间触发
         * User event triggered.
         *
         * @param ctx the ctx
         * @param evt the evt
         * @throws Exception the exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                debugLog("idle:" + evt);
                IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
                if (idleStateEvent.state() == IdleState.READER_IDLE) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("channel:" + ctx.channel() + " read idle.");
                    }
                    // 释放上下文
                    handleDisconnect(ctx);
                    try {
                        // 关闭Channel
                        closeChannelHandlerContext(ctx);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        }

    }

}
