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
package io.seata.saga.engine.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import io.seata.saga.engine.StateMachineConfig;
import io.seata.saga.engine.evaluation.EvaluatorFactoryManager;
import io.seata.saga.engine.evaluation.exception.ExceptionMatchEvaluatorFactory;
import io.seata.saga.engine.evaluation.expression.ExpressionEvaluatorFactory;
import io.seata.saga.engine.expression.ExpressionFactoryManager;
import io.seata.saga.engine.expression.seq.SequenceExpressionFactory;
import io.seata.saga.engine.expression.spel.SpringELExpressionFactory;
import io.seata.saga.engine.invoker.ServiceInvokerManager;
import io.seata.saga.engine.invoker.impl.SpringBeanServiceInvoker;
import io.seata.saga.engine.pcext.StateMachineProcessHandler;
import io.seata.saga.engine.pcext.StateMachineProcessRouter;
import io.seata.saga.engine.repo.StateLogRepository;
import io.seata.saga.engine.repo.StateMachineRepository;
import io.seata.saga.engine.repo.impl.StateLogRepositoryImpl;
import io.seata.saga.engine.repo.impl.StateMachineRepositoryImpl;
import io.seata.saga.engine.sequence.SeqGenerator;
import io.seata.saga.engine.sequence.SpringJvmUUIDSeqGenerator;
import io.seata.saga.engine.store.StateLangStore;
import io.seata.saga.engine.store.StateLogStore;
import io.seata.saga.engine.strategy.StatusDecisionStrategy;
import io.seata.saga.engine.strategy.impl.DefaultStatusDecisionStrategy;
import io.seata.saga.proctrl.ProcessRouter;
import io.seata.saga.proctrl.ProcessType;
import io.seata.saga.proctrl.eventing.impl.AsyncEventBus;
import io.seata.saga.proctrl.eventing.impl.DirectEventBus;
import io.seata.saga.proctrl.eventing.impl.ProcessCtrlEventConsumer;
import io.seata.saga.proctrl.eventing.impl.ProcessCtrlEventPublisher;
import io.seata.saga.proctrl.handler.DefaultRouterHandler;
import io.seata.saga.proctrl.handler.ProcessHandler;
import io.seata.saga.proctrl.handler.RouterHandler;
import io.seata.saga.proctrl.impl.ProcessControllerImpl;
import io.seata.saga.proctrl.process.impl.CustomizeBusinessProcessor;
import io.seata.saga.statelang.domain.DomainConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;

/**
 * Default state machine configuration
 *
 * @author lorne.cl
 */
public class DefaultStateMachineConfig implements StateMachineConfig, ApplicationContextAware, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStateMachineConfig.class);

    private static final int DEFAULT_TRANS_OPER_TIMEOUT     = 60000 * 30;
    private static final int DEFAULT_SERVICE_INVOKE_TIMEOUT = 60000 * 5;

    private int transOperationTimeout = DEFAULT_TRANS_OPER_TIMEOUT;
    private int serviceInvokeTimeout  = DEFAULT_SERVICE_INVOKE_TIMEOUT;

    private StateLogRepository stateLogRepository;
    private StateLogStore stateLogStore;
    private StateLangStore stateLangStore;
    private ExpressionFactoryManager expressionFactoryManager;
    private EvaluatorFactoryManager evaluatorFactoryManager;
    private StateMachineRepository stateMachineRepository;
    private StatusDecisionStrategy statusDecisionStrategy;
    private SeqGenerator seqGenerator;

    private ProcessCtrlEventPublisher syncProcessCtrlEventPublisher;
    private ProcessCtrlEventPublisher asyncProcessCtrlEventPublisher;
    private ApplicationContext applicationContext;
    private ThreadPoolExecutor threadPoolExecutor;
    private boolean enableAsync;
    private ServiceInvokerManager serviceInvokerManager;

    private Resource[] resources = new Resource[0];
    private String charset = "UTF-8";
    private String defaultTenantId = "000001";

    protected void init() throws Exception {

        if (expressionFactoryManager == null) {
            expressionFactoryManager = new ExpressionFactoryManager(); // 实例化表达式工厂管理器

            // spring EL 表达式解析工厂
            SpringELExpressionFactory springELExpressionFactory = new SpringELExpressionFactory();
            springELExpressionFactory.setApplicationContext(getApplicationContext());
            expressionFactoryManager.putExpressionFactory(ExpressionFactoryManager.DEFAULT_EXPRESSION_TYPE,
                springELExpressionFactory);

            // 序列表达式工厂
            SequenceExpressionFactory sequenceExpressionFactory = new SequenceExpressionFactory();
            sequenceExpressionFactory.setSeqGenerator(getSeqGenerator());
            expressionFactoryManager.putExpressionFactory(DomainConstants.EXPRESSION_TYPE_SEQUENCE,
                sequenceExpressionFactory);
        }

        if (evaluatorFactoryManager == null) {
            evaluatorFactoryManager = new EvaluatorFactoryManager(); // 鉴别器工厂管理器

            // EL表达式鉴别工厂
            ExpressionEvaluatorFactory expressionEvaluatorFactory = new ExpressionEvaluatorFactory();
            expressionEvaluatorFactory.setExpressionFactory(
                expressionFactoryManager.getExpressionFactory(ExpressionFactoryManager.DEFAULT_EXPRESSION_TYPE));
            evaluatorFactoryManager.putEvaluatorFactory(EvaluatorFactoryManager.EVALUATOR_TYPE_DEFAULT,
                expressionEvaluatorFactory);

            // 异常匹配鉴别工厂
            evaluatorFactoryManager.putEvaluatorFactory(DomainConstants.EVALUATOR_TYPE_EXCEPTION,
                new ExceptionMatchEvaluatorFactory());
        }

        if (stateMachineRepository == null) {
            // 状态机仓库
            StateMachineRepositoryImpl stateMachineRepository = new StateMachineRepositoryImpl();
            stateMachineRepository.setCharset(charset);
            stateMachineRepository.setSeqGenerator(seqGenerator);
            stateMachineRepository.setStateLangStore(stateLangStore);
            stateMachineRepository.setDefaultTenantId(defaultTenantId);
            if (resources != null) {
                try {
                    // 注册资源
                    stateMachineRepository.registryByResources(resources, defaultTenantId);
                } catch (IOException e) {
                    LOGGER.error("Load State Language Resources failed.", e);
                }
            }
            this.stateMachineRepository = stateMachineRepository;
        }

        if (stateLogRepository == null) {
            // 状态日志仓库
            StateLogRepositoryImpl stateLogRepositoryImpl = new StateLogRepositoryImpl();
            stateLogRepositoryImpl.setStateLogStore(stateLogStore);
            this.stateLogRepository = stateLogRepositoryImpl;
        }

        if (statusDecisionStrategy == null) {
            statusDecisionStrategy = new DefaultStatusDecisionStrategy(); // 状态决策策略
        }

        // 同步事件发布者
        if (syncProcessCtrlEventPublisher == null) {
            // 事件发布者
            ProcessCtrlEventPublisher syncEventPublisher = new ProcessCtrlEventPublisher();

            // 流程控制器
            ProcessControllerImpl processorController = createProcessorController(syncEventPublisher);

            // 事件监听者
            ProcessCtrlEventConsumer processCtrlEventConsumer = new ProcessCtrlEventConsumer();
            processCtrlEventConsumer.setProcessController(processorController);

            // 透传事件BUS
            DirectEventBus directEventBus = new DirectEventBus();
            syncEventPublisher.setEventBus(directEventBus);

            // 注册事件监听者
            directEventBus.registerEventConsumer(processCtrlEventConsumer);

            syncProcessCtrlEventPublisher = syncEventPublisher;
        }

        // 开启异步事件发布
        if (enableAsync && asyncProcessCtrlEventPublisher == null) {
            // 事件发布者
            ProcessCtrlEventPublisher asyncEventPublisher = new ProcessCtrlEventPublisher();

            // 流程控制器
            ProcessControllerImpl processorController = createProcessorController(asyncEventPublisher);

            // 事件监听者
            ProcessCtrlEventConsumer processCtrlEventConsumer = new ProcessCtrlEventConsumer();
            processCtrlEventConsumer.setProcessController(processorController);

            // 异步事件BUS
            AsyncEventBus asyncEventBus = new AsyncEventBus();
            asyncEventBus.setThreadPoolExecutor(getThreadPoolExecutor());
            asyncEventPublisher.setEventBus(asyncEventBus);

            // 注册事件监听者
            asyncEventBus.registerEventConsumer(processCtrlEventConsumer);

            asyncProcessCtrlEventPublisher = asyncEventPublisher;
        }

        // 服务调用管理器
        if (this.serviceInvokerManager == null) {
            this.serviceInvokerManager = new ServiceInvokerManager();

            // Spring Bean调用管理器
            SpringBeanServiceInvoker springBeanServiceInvoker = new SpringBeanServiceInvoker();
            springBeanServiceInvoker.setApplicationContext(getApplicationContext());
            springBeanServiceInvoker.setThreadPoolExecutor(threadPoolExecutor);
            this.serviceInvokerManager
                .putServiceInvoker(DomainConstants.SERVICE_TYPE_SPRING_BEAN, springBeanServiceInvoker);
        }
    }

    private ProcessControllerImpl createProcessorController(ProcessCtrlEventPublisher eventPublisher) throws Exception {

        // 状态机路由
        StateMachineProcessRouter stateMachineProcessRouter = new StateMachineProcessRouter();
        stateMachineProcessRouter.initDefaultStateRouters(); // 初始化默认状态机路由器

        // 状态机处理Handler
        StateMachineProcessHandler stateMachineProcessHandler = new StateMachineProcessHandler();
        stateMachineProcessHandler.initDefaultHandlers(); // 初始化默认处理Handler

        // 默认路由Handler
        DefaultRouterHandler defaultRouterHandler = new DefaultRouterHandler();
        defaultRouterHandler.setEventPublisher(eventPublisher);

        // 处理路由集合
        Map<String, ProcessRouter> processRouterMap = new HashMap<>(1);
        processRouterMap.put(ProcessType.STATE_LANG.getCode(), stateMachineProcessRouter);
        defaultRouterHandler.setProcessRouters(processRouterMap);

        // 定义业务处理器
        CustomizeBusinessProcessor customizeBusinessProcessor = new CustomizeBusinessProcessor();

        // 处理器集合
        Map<String, ProcessHandler> processHandlerMap = new HashMap<>(1);
        processHandlerMap.put(ProcessType.STATE_LANG.getCode(), stateMachineProcessHandler);
        customizeBusinessProcessor.setProcessHandlers(processHandlerMap);

        // 路由处理器集合
        Map<String, RouterHandler> routerHandlerMap = new HashMap<>(1);
        routerHandlerMap.put(ProcessType.STATE_LANG.getCode(), defaultRouterHandler);
        customizeBusinessProcessor.setRouterHandlers(routerHandlerMap);

        // 默认流程控制器
        ProcessControllerImpl processorController = new ProcessControllerImpl();
        processorController.setBusinessProcessor(customizeBusinessProcessor);

        return processorController;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init(); // 初始化
    }

    @Override
    public StateLogStore getStateLogStore() {
        return this.stateLogStore;
    }

    public void setStateLogStore(StateLogStore stateLogStore) {
        this.stateLogStore = stateLogStore;
    }

    @Override
    public StateLangStore getStateLangStore() {
        return stateLangStore;
    }

    public void setStateLangStore(StateLangStore stateLangStore) {
        this.stateLangStore = stateLangStore;
    }

    @Override
    public ExpressionFactoryManager getExpressionFactoryManager() {
        return this.expressionFactoryManager;
    }

    public void setExpressionFactoryManager(ExpressionFactoryManager expressionFactoryManager) {
        this.expressionFactoryManager = expressionFactoryManager;
    }

    @Override
    public EvaluatorFactoryManager getEvaluatorFactoryManager() {
        return this.evaluatorFactoryManager;
    }

    public void setEvaluatorFactoryManager(EvaluatorFactoryManager evaluatorFactoryManager) {
        this.evaluatorFactoryManager = evaluatorFactoryManager;
    }

    @Override
    public String getCharset() {
        return this.charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public StateMachineRepository getStateMachineRepository() {
        return stateMachineRepository;
    }

    public void setStateMachineRepository(StateMachineRepository stateMachineRepository) {
        this.stateMachineRepository = stateMachineRepository;
    }

    @Override
    public StatusDecisionStrategy getStatusDecisionStrategy() {
        return statusDecisionStrategy;
    }

    public void setStatusDecisionStrategy(StatusDecisionStrategy statusDecisionStrategy) {
        this.statusDecisionStrategy = statusDecisionStrategy;
    }

    @Override
    public SeqGenerator getSeqGenerator() {
        if (seqGenerator == null) {
            synchronized (this) {
                if (seqGenerator == null) {
                    seqGenerator = new SpringJvmUUIDSeqGenerator();
                }
            }
        }
        return seqGenerator;
    }

    public void setSeqGenerator(SeqGenerator seqGenerator) {
        this.seqGenerator = seqGenerator;
    }

    @Override
    public ProcessCtrlEventPublisher getProcessCtrlEventPublisher() {
        return syncProcessCtrlEventPublisher;
    }

    @Override
    public ProcessCtrlEventPublisher getAsyncProcessCtrlEventPublisher() {
        return asyncProcessCtrlEventPublisher;
    }

    public void setAsyncProcessCtrlEventPublisher(ProcessCtrlEventPublisher asyncProcessCtrlEventPublisher) {
        this.asyncProcessCtrlEventPublisher = asyncProcessCtrlEventPublisher;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

    public void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    public boolean isEnableAsync() {
        return enableAsync;
    }

    public void setEnableAsync(boolean enableAsync) {
        this.enableAsync = enableAsync;
    }

    @Override
    public StateLogRepository getStateLogRepository() {
        return stateLogRepository;
    }

    public void setStateLogRepository(StateLogRepository stateLogRepository) {
        this.stateLogRepository = stateLogRepository;
    }

    public void setSyncProcessCtrlEventPublisher(ProcessCtrlEventPublisher syncProcessCtrlEventPublisher) {
        this.syncProcessCtrlEventPublisher = syncProcessCtrlEventPublisher;
    }

    public void setResources(Resource[] resources) {
        this.resources = resources;
    }

    @Override
    public ServiceInvokerManager getServiceInvokerManager() {
        return serviceInvokerManager;
    }

    public void setServiceInvokerManager(ServiceInvokerManager serviceInvokerManager) {
        this.serviceInvokerManager = serviceInvokerManager;
    }

    @Override
    public String getDefaultTenantId() {
        return defaultTenantId;
    }

    public void setDefaultTenantId(String defaultTenantId) {
        this.defaultTenantId = defaultTenantId;
    }

    @Override
    public int getTransOperationTimeout() {
        return transOperationTimeout;
    }

    public void setTransOperationTimeout(int transOperationTimeout) {
        this.transOperationTimeout = transOperationTimeout;
    }

    @Override
    public int getServiceInvokeTimeout() {
        return serviceInvokeTimeout;
    }

    public void setServiceInvokeTimeout(int serviceInvokeTimeout) {
        this.serviceInvokeTimeout = serviceInvokeTimeout;
    }
}