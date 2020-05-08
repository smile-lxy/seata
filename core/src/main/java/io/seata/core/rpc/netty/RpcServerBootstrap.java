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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.rpc.RemotingServer;
import io.seata.core.rpc.netty.v1.ProtocolV1Decoder;
import io.seata.core.rpc.netty.v1.ProtocolV1Encoder;
import io.seata.discovery.registry.RegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Rpc server bootstrap.
 *
 * @author zhangchenghui.dev@gmail.com
 * @since 1.1.0
 */
public class RpcServerBootstrap implements RemotingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServerBootstrap.class);
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    private ChannelHandler[] channelHandlers;
    private int listenPort;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public RpcServerBootstrap(NettyServerConfig nettyServerConfig) {

        this.nettyServerConfig = nettyServerConfig;
        // 根本配置选择不同负载模式
        if (NettyServerConfig.enableEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new EpollEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                    nettyServerConfig.getServerWorkerThreads()));
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                    nettyServerConfig.getServerWorkerThreads()));
        }

        // 配置默认监听端口
        // init listenPort in constructor so that getListenPort() will always get the exact port
        setListenPort(nettyServerConfig.getDefaultListenPort());
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    protected void setChannelHandlers(final ChannelHandler... handlers) {
        if (null != handlers) {
            channelHandlers = handlers;
        }
    }

    /**
     * Add channel pipeline last.
     *
     * @param channel  the channel
     * @param handlers the handlers
     */
    private void addChannelPipelineLast(Channel channel, ChannelHandler... handlers) {
        if (null != channel && null != handlers) {
            channel.pipeline().addLast(handlers);
        }
    }

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {

        if (listenPort <= 0) {
            throw new IllegalArgumentException("listen port: " + listenPort + " is invalid!");
        }
        this.listenPort = listenPort;
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return listenPort;
    }

    @Override
    public void start() {
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
            // Channel选择器
            .channel(NettyServerConfig.SERVER_CHANNEL_CLAZZ)
            // 服务器请求处理线程全满时, 临时存放已完成三次握手的请求的队列最大大小
            .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getSoBackLogSize())
            // 是否允许公用端口
            .option(ChannelOption.SO_REUSEADDR, true)
            // 是否启动心跳保活机制
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            // 是否尽可能利用网络带宽, 尽可能发送足够大的数据包(要求高实时性(有数据就发): true, 减少网络交互: false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            // 发送消息时系统缓存区大小
            .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSendBufSize())
            // 接受消息时系统缓存区大小
            .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketResvBufSize())
            // 向channel写数据时, 缓冲区边界(https://www.iteye.com/blog/laolinshi-2341729)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(),
                    nettyServerConfig.getWriteBufferHighWaterMark()))
            .localAddress(new InetSocketAddress(listenPort))
            .childHandler(new ChannelInitializer<SocketChannel>() { // client连接后handler链
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new IdleStateHandler(nettyServerConfig.getChannelMaxReadIdleSeconds(), 0, 0)) //心跳包
                        .addLast(new ProtocolV1Decoder()) // 数据包解码器
                        .addLast(new ProtocolV1Encoder()); // 数据包编码器
                    if (null != channelHandlers) {
                        // 添加额外handler链, 目前只有'AbstractRpcRemotingServer.ServerHandler'
                        addChannelPipelineLast(ch, channelHandlers);
                    }

                }
            });

        try {
            ChannelFuture future = this.serverBootstrap.bind(listenPort).sync();
            LOGGER.info("Server started ... ");
            // 向注册中心注册
            RegistryFactory.getInstance().register(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
            // RPC 初始化成功
            initialized.set(true);
            // 一直等待, 直到Socket被关闭
            future.channel().closeFuture().sync();
        } catch (Exception exx) {
            throw new RuntimeException(exx);
        }

    }

    @Override
    public void shutdown() {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Shutting server down. ");
            }
            if (initialized.get()) {
                // 通知对应注册中心实现, 取消注册
                RegistryFactory.getInstance().unregister(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
                RegistryFactory.getInstance().close();
                //wait a few seconds for server transport
                TimeUnit.SECONDS.sleep(nettyServerConfig.getServerShutdownWaitTime());
            }

            // 优雅关闭
            this.eventLoopGroupBoss.shutdownGracefully();
            this.eventLoopGroupWorker.shutdownGracefully();
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }
}
