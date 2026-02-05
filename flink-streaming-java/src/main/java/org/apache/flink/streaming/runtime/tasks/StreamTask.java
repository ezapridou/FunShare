/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import net.michaelkoepf.spegauge.api.sut.DataDistrSplitStats;
import net.michaelkoepf.spegauge.api.sut.FilterDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.JoinDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.JoinMonitor;
import net.michaelkoepf.spegauge.api.sut.QuerySetAssigner;
import net.michaelkoepf.spegauge.api.sut.ReconfigurableSourceData;

import net.michaelkoepf.spegauge.api.sut.SelectivitiesMonitor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.extensions.controller.StartMonitoringDataDistrControlMessage;
import org.apache.flink.extensions.controller.StopMonitoringDataDistrControlMessage;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.extensions.controller.ControlMessage;
import org.apache.flink.extensions.controller.MIGRATION_TYPE_FROM_PASSIVE_QUERY;
import org.apache.flink.extensions.controller.ReconfigurationControlMessage;
import org.apache.flink.extensions.controller.TaskUtilizationStats;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.security.FlinkSecurityManager;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction.Suspension;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.concurrent.FutureUtils.assertNoException;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.ExceptionUtils.rethrowException;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> close-operators()
 *       +----> dispose-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>> extends AbstractInvokable
        implements AsyncExceptionHandler {

    /** The thread group that holds all trigger timer threads. */
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);
    private static final Marker GROUP_SHARE = MarkerFactory.getMarker("GroupShare");

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    protected final StateBackend stateBackend;

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    protected final CheckpointStorage checkpointStorage;

    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    private volatile boolean isRunning;

    /** Flag to mark this task as canceled. */
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    private volatile boolean failing;

    private boolean disposedOperators;

    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;

    private final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

    protected final MailboxProcessor mailboxProcessor;

    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    private final ExecutorService channelIOExecutor;

    private Long syncSavepointId = null;
    private Long activeSyncSavepointId = null;
    private long latestAsyncCheckpointStartDelayNanos;
    private Timer timer = new Timer();
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    /** Connection to the task manager. */
    private TaskManagerActions taskManagerActions;

    /**
     * Boolean that signifies whether the task is blocked waiting to receive state
     * (state migration has not been completed)
     * This boolean is for state that is in-memory (will be received by the Task Manager)
     */
    private volatile boolean isWaitingForInMemState = false;

    /**
     * Boolean that signifies whether the task is blocked waiting to receive state
     * (state migration has not been completed)
     * This boolean is for state that is serialized (will be received by the Job Manager)
     */
    private boolean isWaitingForSerializedState = false;

    /**
     * The control message that is pending to be sent downstream. This is used from the receiver
     * task of state migration to send the control message downstream once state migration is
     * completed.
     */
    private ReconfigurationControlMessage pendingControlMessage = null;

    /**
     * This boolean is true if the dop must be changed when the pending control message is emitted
     */
    private boolean changeDOPafterPendingControlMessage = false;

    /**
     * Indicates whether the pointer to the streamTask has been set in the SourceFunction.
     * This is used during unsharing for the SourceFunction to instruct the streamTask to send the
     * pending control message once the barrier from the data generators is received.
     */
    private boolean pointerInSourceFunctionSet = false;

    private boolean pendingSerializedStateBool = false;
    private Map<Integer, Map<Integer, byte[]>> pendingSerializedState;
    private Map<Integer, HashMap<byte[], byte[]>> pendingSerializedDedupMaps;
    private List<byte[]> pendingSerializedTriggers;
    private Map<Integer, Map<Integer, byte[]>> pendingSerializedStatePassiveQuery;
    private Map<Integer, String> pendingSerializedStateNameDict;

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     */
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {

        super(environment);

        this.configuration = new StreamConfig(getTaskConfiguration());
        String id = getEnvironment().getJobVertexId().toString();
        TaskInfo info = getEnvironment().getTaskInfo();
        JobVertexID jobVId = getEnvironment().getJobVertexId();
        int subtaskIdx = info.getIndexOfThisSubtask();
        Map<String, String> globalArgs = environment.getExecutionConfig().getGlobalJobParameters().toMap();
        this.recordWriter = createRecordWriterDelegate(configuration, environment);
        this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
        this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor, environment.getTaskInfo().getTaskNameWithSubtasks());
        this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
        this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
        this.asyncOperationsThreadPool =
                Executors.newCachedThreadPool(
                        new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

        this.stateBackend = createStateBackend();
        this.checkpointStorage = createCheckpointStorage(stateBackend);

        this.subtaskCheckpointCoordinator =
                new SubtaskCheckpointCoordinatorImpl(
                        checkpointStorage.createCheckpointStorage(getEnvironment().getJobID()),
                        getName(),
                        actionExecutor,
                        getCancelables(),
                        getAsyncOperationsThreadPool(),
                        getEnvironment(),
                        this,
                        configuration.isUnalignedCheckpointsEnabled(),
                        this::prepareInputSnapshot);

        // if the clock is not already set, then assign a default TimeServiceProvider
        if (timerService == null) {
            this.timerService = createTimerService("Time Trigger for " + getName());
        } else {
            this.timerService = timerService;
        }
        this.systemTimerService = createTimerService("System Time Trigger for " + getName());
        this.channelIOExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("channel-state-unspilling"));

        injectChannelStateWriterIntoChannels();
        final String name = info.getTaskName();

        environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
    }

    @Override
    public void setTaskManagerActions(TaskManagerActions taskManagerActions) {
        this.taskManagerActions = taskManagerActions;
    }

    @Override
    public void sendStateToReceiver(Map<String, Map<Integer, StateMap<?, ?, ?>>> state,
                                    Map<String, Map<Integer, StateMap<?, ?, ?>>> statePassiveQuery,
                                    Map<Integer, HashMap<?, ?>> dedupMaps,
                                    List<HeapPriorityQueueElement> triggers) {
        assert isWaitingForInMemState;
        if (state != null) {
            LOG.debug(GROUP_SHARE, "The state is not null " + getEnvironment().getTaskInfo().getTaskName()
                    + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        }
        else {
            LOG.debug(GROUP_SHARE, "The state is null " + getEnvironment().getTaskInfo().getTaskName()
                    + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        }

        if (mainOperator instanceof AbstractStreamOperator){
            if (pendingControlMessage == null ) {
                throw new RuntimeException("The pending control message is null " + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            }
            int index = pendingControlMessage.activeChannelsOfActiveGroup.indexOf(
                    getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            ((AbstractStreamOperator) mainOperator).getRuntimeContext()
                    .incorporateReceivedState(state, statePassiveQuery,
                            pendingControlMessage.newDOPActive, index);
            if (mainOperator instanceof WindowOperator || mainOperator instanceof KeyedCoProcessOperator) {
                LOG.debug(GROUP_SHARE, "This is a windowed operator " + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
                InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                        ((WindowOperator) mainOperator).getInternalTimerService() :
                        ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

                timerService.incorporateReceivedWindowState(
                        dedupMaps,
                        triggers,
                        pendingControlMessage.newDOPActive,
                        index,
                        pendingControlMessage.maxParallelism,
                        pendingControlMessage.oldDOPActive);
            }
        }

        if (mainOperator instanceof KeyedCoProcessOperator) {
            if (((AbstractUdfStreamOperator) mainOperator).getUserFunction()
                    instanceof KeyedCoProcessFunction) {
                LOG.debug(GROUP_SHARE, "This is a KeyedCoProcessFunction! " + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            }
        }
        isWaitingForInMemState = false;
        tryToSendPendingControlMessage();

        if (pendingSerializedStateBool) {
            incorporateSerializedState(pendingSerializedState, pendingSerializedDedupMaps,
                    pendingSerializedTriggers, pendingSerializedStatePassiveQuery,
                    pendingSerializedStateNameDict);
        }
    }

    @Override
    public void sendStateToReceiverDownstream(Map<String, StateMap<?, ?, ?>[]> state,
                                              HashMap<?, ?>[] dedupMaps,
                                              HeapPriorityQueueElement[] triggers, int queueSize) {
        if (state != null) {
            LOG.debug(GROUP_SHARE, "The state is not null " + getEnvironment().getTaskInfo().getTaskName()
                    + " " + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            if (mainOperator instanceof AbstractStreamOperator){
                ((AbstractStreamOperator) mainOperator).getRuntimeContext()
                        .incorporateReceivedState(state);
                if (mainOperator instanceof WindowOperator && dedupMaps != null) {
                    //LOG.debug(GROUP_SHARE, "The downstream receiver is a WindowOperator");
                    ((WindowOperator) mainOperator).incorporateReceivedWindowState(
                            dedupMaps,
                            triggers,
                            queueSize);
                }
            }
        }
        else {
            LOG.debug(GROUP_SHARE, "The state is null " + getEnvironment().getTaskInfo().getTaskName()
                    + " " + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        }

        isWaitingForInMemState = false;
        tryToSendPendingControlMessage();
    }

    @Override
    public void sendSerializedStateToReceiver(Map<Integer, Map<Integer, byte[]>> state,
                                              Map<Integer, HashMap<byte[], byte[]>> dedupMaps,
                                              List<byte[]> triggers,
                                              Map<Integer, Map<Integer, byte[]>> statePassiveQuery,
                                              Map<Integer, String> stateNameDict) {
        assert isWaitingForSerializedState;
        if (isWaitingForInMemState) {
            pendingSerializedStateBool = true;
            pendingSerializedState = state;
            pendingSerializedDedupMaps = dedupMaps;
            pendingSerializedTriggers = triggers;
            pendingSerializedStatePassiveQuery = statePassiveQuery;
            pendingSerializedStateNameDict = stateNameDict;
        }
        else {
            incorporateSerializedState(state, dedupMaps, triggers, statePassiveQuery, stateNameDict);
        }
    }

    private void incorporateSerializedState(Map<Integer, Map<Integer, byte[]>> state,
                                            Map<Integer, HashMap<byte[], byte[]>> dedupMaps,
                                            List<byte[]> triggers,
                                            Map<Integer, Map<Integer, byte[]>> statePassiveQuery,
                                            Map<Integer, String> stateNameDict) {
        if (mainOperator instanceof AbstractStreamOperator) {
            if (state != null) {
                LOG.debug(GROUP_SHARE, "The state from the Job Manager is not null "
                        + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            } else {
                LOG.debug(GROUP_SHARE, "The state from the Job Manager is null "
                        + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            }
            ((AbstractStreamOperator) mainOperator).getRuntimeContext()
                    .incorporateReceivedSerializedState(state, statePassiveQuery, stateNameDict);
            if ((mainOperator instanceof WindowOperator || mainOperator instanceof KeyedCoProcessOperator)
                    && dedupMaps != null) {
                InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                        ((WindowOperator) mainOperator).getInternalTimerService() :
                        ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

                timerService.incorporateReceivedSerializedWindowState(dedupMaps, triggers);
            }
        }
        isWaitingForSerializedState = false;
        if (pendingSerializedStateBool) {
            pendingSerializedState = null;
            pendingSerializedDedupMaps = null;
            pendingSerializedTriggers = null;
            pendingSerializedStatePassiveQuery = null;
            pendingSerializedStateNameDict = null;
            pendingSerializedStateBool = false;
        }
        tryToSendPendingControlMessage();
    }

    @Override
    public void sendSerializedStateToReceiverDownstream(Map<String, byte[][]> state,
                                                        HashMap<byte[], byte[]>[] dedupMaps,
                                                        byte[][] triggers, int queueSize) {
        if (isWaitingForInMemState) {
            throw new IllegalStateException("The task received serialized state before in memory state. "
                    + "The current implementation does not support this.");
        }

        if (mainOperator instanceof AbstractStreamOperator) {
            if (state != null) {
                ((AbstractStreamOperator) mainOperator).getRuntimeContext()
                        .incorporateReceivedSerializedStateDownstream(state);

                if (mainOperator instanceof WindowOperator
                        || mainOperator instanceof KeyedCoProcessOperator) {
                    InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                            ((WindowOperator) mainOperator).getInternalTimerService() :
                            ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

                    timerService.incorporateReceivedSerializedWindowStateDownstream(dedupMaps, triggers, queueSize);
                }
            }
        }
        isWaitingForInMemState = false;
        tryToSendPendingControlMessage();
    }

    @Override
    public void sendControl(ControlMessage controlMessage) {
        if (controlMessage instanceof ReconfigurationControlMessage) {
            sendReconfControl((ReconfigurationControlMessage) controlMessage);
        }
        else if (controlMessage instanceof StartMonitoringDataDistrControlMessage) {
            LOG.debug(GROUP_SHARE, "Received start monitoring control message merge - "
                    + getEnvironment().getTaskInfo().getTaskName()
                    + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            StartMonitoringDataDistrControlMessage monitoringControlMessage =
                    (StartMonitoringDataDistrControlMessage) controlMessage;
            SelectivitiesMonitor selectivitiesMonitorOfCurrentTask = getSelectivitiesMonitor();

            if (selectivitiesMonitorOfCurrentTask != null){
                selectivitiesMonitorOfCurrentTask.enableMonitoring(
                        monitoringControlMessage.queryToDataRangesMap,
                        monitoringControlMessage.dataRanges);
                sendMonitoringControlMessageDownstream(monitoringControlMessage);
            } else {
                JoinMonitor joinMonitorOfCurrentTask = getJoinMonitor();
                if (joinMonitorOfCurrentTask != null) {
                    joinMonitorOfCurrentTask.enableMonitoringMerge(
                            monitoringControlMessage.queryToDataRangesMap,
                            monitoringControlMessage.dataRanges,
                            monitoringControlMessage.activeQueries);
                } else {
                    sendMonitoringControlMessageDownstream(monitoringControlMessage);
                }
            }
        }
        else if (controlMessage instanceof StopMonitoringDataDistrControlMessage) {
            LOG.debug(GROUP_SHARE, "Received stop monitoring control message merge - "
                    + getEnvironment().getTaskInfo().getTaskName()
                    + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            JoinMonitor joinMonitor = getJoinMonitor();
            if (joinMonitor != null) {
                joinMonitor.disableMonitoringMerge();
            }
        }
        else {
            throw new UnsupportedOperationException("Control message type not supported");
        }
    }

    @Override
    public FilterDataDistrMergeStats sendStopDataDistrControlFilter(StopMonitoringDataDistrControlMessage controlMessage){
        LOG.debug(GROUP_SHARE, "Received filter stop monitoring control message merge - "
                + getEnvironment().getTaskInfo().getTaskName()
                + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        SelectivitiesMonitor monitoringOperator = getSelectivitiesMonitor();
        if (monitoringOperator == null) {
            JoinMonitor joinMonitorOfCurrentTask = getJoinMonitor();
            if (joinMonitorOfCurrentTask != null) {
                joinMonitorOfCurrentTask.disableMonitoringMerge();
            }
            //sendMonitoringControlMessageDownstream(controlMessage);
            return null;
        }
        sendMonitoringControlMessageDownstream(controlMessage);
        return monitoringOperator.getFilterDataDistrStats();
    }

    @Override
    public JoinDataDistrMergeStats sendStopDataDistrControlJoin(StopMonitoringDataDistrControlMessage controlMessage){
        LOG.debug(GROUP_SHARE, "Received join stop monitoring control message merge - "
                + getEnvironment().getTaskInfo().getTaskName()
                + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        JoinMonitor monitoringOperator = getJoinMonitor();
        if (monitoringOperator == null) {
            throw new IllegalStateException("Stop monitoring data distribution control message "
                    + "received but no monitoring operator found.");
        }
        return monitoringOperator.getJoinDataDistrMergeStats();
    }

    private SelectivitiesMonitor getSelectivitiesMonitor() {
        for (StreamOperatorWrapper operator : operatorChain.getAllOperators()){
            if (operator.getStreamOperator() instanceof AbstractUdfStreamOperator) {
                if (((AbstractUdfStreamOperator) operator.getStreamOperator()).getUserFunction()
                        instanceof SelectivitiesMonitor) {
                    return (SelectivitiesMonitor) ((AbstractUdfStreamOperator) operator.getStreamOperator()).getUserFunction();
                }
            }
        }
        return null;
    }

    private JoinMonitor getJoinMonitor() {
        for (StreamOperatorWrapper operator : operatorChain.getAllOperators()){
            if (operator.getStreamOperator() instanceof AbstractUdfStreamOperator) {
                if (((AbstractUdfStreamOperator) operator.getStreamOperator()).getUserFunction()
                        instanceof JoinMonitor) {
                    return (JoinMonitor) ((AbstractUdfStreamOperator) operator.getStreamOperator()).getUserFunction();
                }
            }
        }
        return null;
    }

    public void sendReconfControl(ReconfigurationControlMessage controlMessage) {
        TaskInfo info = getEnvironment().getTaskInfo();
        int taskIndex = info.getIndexOfThisSubtask();
        String taskNameIdx = info.getTaskName() + "-" + taskIndex;
        if (mainOperator instanceof AbstractStreamOperator && !(recordWriter instanceof NonRecordWriter)){
            //((AbstractStreamOperator) mainOperator).printOutputClass();
            LOG.debug(GROUP_SHARE, taskNameIdx + " recordWriter: " + recordWriter.getRecordWriter(0).getClass().getName());
        }

        /* if (inputProcessor != null && !taskNameIdx.startsWith("Source")) {
            if (inputProcessor instanceof StreamOneInputProcessor || inputProcessor instanceof StreamTwoInputProcessor
                || inputProcessor instanceof StreamMultipleInputProcessor) {
                LOG.debug(GROUP_SHARE, taskNameIdx + " inputProcessor: " + inputProcessor.printClass());
            }
            else if (inputProcessor != null){
                LOG.debug(GROUP_SHARE, taskNameIdx + " inputProcessor: " + inputProcessor.getClass().getName());
            }
        }*/
        /* for (StreamOperatorWrapper operator : operatorChain.getAllOperators()){
            if (operator.getStreamOperator() instanceof AbstractStreamOperator){
                ((AbstractStreamOperator) operator.getStreamOperator()).printOutputClass();
            }
        }*/

        assert (taskManagerActions != null);

        boolean emitControlMessageDirectly = true;
        boolean sendLastTupleBeforeEmittingCtrlMsg = false;
        boolean changeDOP = false;

        // ---------- RECEIVER TASK ----------
        // the same task can be both sender and receiver
        if (controlMessage.receivers.contains(taskNameIdx) ||
                controlMessage.downstreamReceivers.contains(taskNameIdx)){
            emitControlMessageDirectly = false;
            LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a receiver");
            sendControlReceiver(controlMessage, taskNameIdx, taskIndex, controlMessage.downstreamReceivers.contains(taskNameIdx));
        }
        // ---------- SENDER TASK ----------
        if (controlMessage.senders.contains(taskNameIdx)){
            LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a sender");
            sendStateToTaskManager(controlMessage, taskNameIdx, taskIndex);
        }

        // ---------- SENDER TASK OF QUERY BEING ADDED TO GROUP (Passive query) ----------
        if (controlMessage.sendersOfPassiveQuery.contains(taskNameIdx)) {
            LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a sender of the passive group ");
            sendStateToTaskManagerPassiveQuery(controlMessage, taskNameIdx, taskIndex);
        }

        // ---------- SOURCE TASK ----------
        if (controlMessage.sourceToResourceIDMap != null &&
                controlMessage.sourceToResourceIDMap.containsKey(taskNameIdx)) {
            ResourceID resourceID = controlMessage.sourceToResourceIDMap.get(taskNameIdx);
            if (controlMessage.sourcesOfActiveQuery.get(resourceID) != null &&
                    controlMessage.sourcesOfActiveQuery.get(resourceID).contains(taskNameIdx)) {
                assert (mainOperator instanceof AbstractUdfStreamOperator &&
                        (((AbstractUdfStreamOperator) mainOperator).getUserFunction()
                                instanceof RichParallelSourceFunction));
                LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is an active source");
                if (controlMessage.share) {
                    sendLastTupleBeforeEmittingCtrlMsg = true;
                } else {
                    AbstractUdfStreamOperator udfOperator = (AbstractUdfStreamOperator) mainOperator;
                    if (taskIndex == 0) {
                        // get current state
                        ReconfigurableSourceData currentState =
                                ((RichParallelSourceFunction) udfOperator.getUserFunction()).getData();
                        String driverPort = String.valueOf(controlMessage.driverPort);
                        // send the driver job id to the task manager
                        LOG.debug(GROUP_SHARE, taskNameIdx + " send driver job id to task manager");
                        taskManagerActions.sendDriverJobIDAndHostname(
                                currentState.getDriverJobId(),
                                currentState.getDriverHostname(),
                                driverPort,
                                controlMessage.sourcesOfPassiveQuery.keySet(),
                                controlMessage.activeGroupId);
                    }
                    if (!pointerInSourceFunctionSet) {
                        // set pointer to the streamTask in the SourceFunction
                        ((RichParallelSourceFunction) udfOperator.getUserFunction())
                                .setStreamTaskPointer(this);
                        pointerInSourceFunctionSet = true;
                    }
                    // The control message will be sent when the barrier from data generators is aligned
                    emitControlMessageDirectly = false;
                    pendingControlMessage = controlMessage;
                    changeDOPafterPendingControlMessage = true;
                }
            } else if (controlMessage.sourcesOfPassiveQuery.get(resourceID) != null &&
                    controlMessage.sourcesOfPassiveQuery.get(resourceID).contains(taskNameIdx)) {
                LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a passive source");
                taskManagerActions.sendSourceTaskExecutionAttemptID(taskIndex,
                        getEnvironment().getExecutionId(),
                        controlMessage.sourcesOfPassiveQuery.get(resourceID).size(),
                        controlMessage.sourcesOfPassiveQuery.size(),
                        resourceID, controlMessage.activeGroupId);
                emitControlMessageDirectly = false;
                this.pendingControlMessage = controlMessage;
            }
        }

        // ---------- UPSTREAM TASK ----------
        if (controlMessage.upstreamTasks.contains(taskNameIdx)) {
            if (mainOperator instanceof AbstractStreamOperator && ((AbstractStreamOperator) mainOperator).hasForwardPartitioner()) {
                LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is an upstream task with a forward partitioner");
            }
            else {
                LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is an upstream task");
                changeDOP = true;
                if (!emitControlMessageDirectly) {
                    changeDOPafterPendingControlMessage = true;
                }
            }
        }

        // ---------- TASK THAT MUST CHANGE THE STATUS OF INPUT CHANNELS ----------
        /*
        There is an alternative implementation for correct watermark propagation while reconfiguring
        the query plans. However, that implementation is not compatible with the current
        implementation of Watermark generation of SPEGauge.
        More precisely, to use the alt implementation you must assign watermarks with a
        WatermarkStrategy withIdleness and make sure watermarks get generated periodically

        The alt implementation is marked with comments in the ChannelSelectorRecordWriter class.
        You also need to comment the lines bellow.

        The current implementation for watermark generation of SPEGauge has the advantage that
        watermarks are deterministic and all queries will generate exactly the same watermarks.
        For this reason, we have not switched to the alt implementation.
         */
        // If for debugging purposes you want to monitor the broadcasting and receiving of watermarks,
        // you can uncomment the lines starting with "GroupShare debug print" in the
        // ChannelSelectorRecordWriter & StatusWatermarkValve class

        // change state of input channels
        if (controlMessage.tasksToChangeInputStatus.contains(taskNameIdx)) {
            if (inputProcessor instanceof StreamOneInputProcessor ||
                    inputProcessor instanceof StreamTwoInputProcessor) {
                LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " will update the status of input channels");
                try {
                    // Set the input channels active/inactive
                    // This is important for the watermark mechanism to work correctly
                    List<Integer> activeChannels = controlMessage.isGroupActive ?
                            controlMessage.activeChannelsOfActiveGroup : controlMessage.activeChannelsOfPassiveGroup;
                    if (inputProcessor instanceof StreamOneInputProcessor) {
                        ((StreamOneInputProcessor) inputProcessor).changeChannelsState(
                                activeChannels);
                    } else {
                        ((StreamTwoInputProcessor) inputProcessor).changeChannelsState(
                                activeChannels);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                throw new UnsupportedOperationException("The input processor is not a StreamOneInputProcessor but a "
                        + inputProcessor.getClass().getName()
                        + ". Setting status of input channels not supported");
            }
        }

        // ---------- Query set Assigner TASK ----------
        // Checking whether the taskName **contains** "QuerySetAssigner" instead of startsWith allows
        // us to chain the QuerySetAssigner with other operators.
        if (controlMessage.querySet != null && taskNameIdx.contains("QuerySetAssigner")) {
            LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a QuerySetAssigner");
            for (StreamOperatorWrapper operator : operatorChain.getAllOperators()){
                LOG.debug(GROUP_SHARE, "Setting querySet");
                if (operator.getStreamOperator() instanceof AbstractUdfStreamOperator) {
                    if (((AbstractUdfStreamOperator) operator.getStreamOperator()).getUserFunction()
                            instanceof QuerySetAssigner) {
                        ((QuerySetAssigner) ((AbstractUdfStreamOperator) operator.getStreamOperator())
                                .getUserFunction()).setActiveQueries(controlMessage.querySet);
                        break;
                    }
                }
            }
        }

        // ---------- DOWNSTREAM SENDER TASK ----------
        if (controlMessage.downstreamSenders.contains(taskNameIdx)){
            LOG.debug(GROUP_SHARE, "State migration: Task " + taskNameIdx + " is a downstream sender");
            sendStateToTaskManagerDownstream(controlMessage, taskNameIdx, taskIndex);
        }

        if (emitControlMessageDirectly) {
            if(!isRunning){return;}

            sendControlMessageDownstream(controlMessage, sendLastTupleBeforeEmittingCtrlMsg, changeDOP);
        }
    }

    private void sendStateToTaskManager(ReconfigurationControlMessage controlMessage, String taskNameIdx, int taskIndex) {
        // TODO GroupShare this logic should be in the mailbox thread?
        if (mainOperator instanceof AbstractStreamOperator){
            // ((AbstractStreamOperator) mainOperator).getRuntimeContext().printState(taskNameIdx);

            // task manager id -> partition -> state id -> key group id -> state
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs =
                    new HashMap<>(2);
            Map<Integer, String> stateNameDict = new HashMap<>(2);
            // get state to be migrated
            Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state =
                    ((AbstractStreamOperator) mainOperator)
                            .getRuntimeContext()
                            .getEntriesForMigration(controlMessage.newDOPActive,
                                    taskIndex,
                                    controlMessage.maxParallelism,
                                    controlMessage.partitionToResourceIDMap,
                                    stateForOtherTMs, stateNameDict,
                                    controlMessage.activeChannelsOfActiveGroup);

            assert controlMessage.numOfSenders != null;
            assert controlMessage.numOfReceivers != null;
            int numOfSenders = getNumOfTasks(controlMessage.numOfSenders, taskNameIdx,
                    controlMessage.taskToResourceIDMap);
            int numOfReceivers = getNumOfTasks(controlMessage.numOfReceivers, taskNameIdx,
                    controlMessage.taskToResourceIDMap);

            if (mainOperator instanceof WindowOperator || mainOperator instanceof KeyedCoProcessOperator) {
                LOG.debug(GROUP_SHARE, "This is a " + mainOperator.getClass().getName() + " " + getEnvironment().getTaskInfo().getTaskName()
                        + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
                InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                        ((WindowOperator) mainOperator).getInternalTimerService() :
                        ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();
                Map<ResourceID, Map<Integer, Map<Integer, HashMap<?, ?>>>> dedupMapsForOtherTMs =
                        new HashMap<>();
                Map<Integer, Map<Integer, HashMap<?, ?>>> dedupMaps = timerService.getDeduplicationMapsForMigration(
                        controlMessage.newDOPActive, controlMessage.oldDOPActive, taskIndex,
                        controlMessage.maxParallelism,
                        controlMessage.partitionToResourceIDMap,
                        dedupMapsForOtherTMs, controlMessage.activeChannelsOfActiveGroup);
                Map<ResourceID, Map<Integer, List<HeapPriorityQueueElement>>> triggersForOtherTMs =
                        new HashMap<>();
                Map<Integer, List<HeapPriorityQueueElement>> triggers =
                        timerService.getElementsForMigration(
                                controlMessage.newDOPActive, taskIndex,
                                controlMessage.maxParallelism,
                                controlMessage.partitionToResourceIDMap,
                                triggersForOtherTMs,
                                controlMessage.activeChannelsOfActiveGroup);

                taskManagerActions.sendState(taskIndex, state, stateForOtherTMs, dedupMaps, triggers,
                        serializeDedupMaps(dedupMapsForOtherTMs), serializeTriggers(triggersForOtherTMs),
                        numOfSenders, numOfReceivers, stateNameDict,
                        controlMessage.multipleTMsInvolvedInMigration,
                        controlMessage.numberOfSenderTMsInvolvedInMigration,
                        controlMessage.activeGroupId);
            }
            else {
                // send the state to the task manager
                taskManagerActions.sendState(taskIndex, state, stateForOtherTMs,
                        numOfSenders, numOfReceivers, stateNameDict,
                        controlMessage.multipleTMsInvolvedInMigration,
                        controlMessage.numberOfSenderTMsInvolvedInMigration,
                        controlMessage.activeGroupId);
            }
        }
        else {
            throw new RuntimeException("The target operator is not a stream operator." + taskNameIdx);
        }
    }

    private Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> serializeDedupMaps(
            Map<ResourceID, Map<Integer, Map<Integer, HashMap<?, ?>>>> dedupMaps) {
        InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                ((WindowOperator) mainOperator).getInternalTimerService() :
                ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

        TypeSerializer keySerializer = timerService.getKeySerializer().duplicate();
        TypeSerializer namespaceSerializer = timerService.getNamespaceSerializer().duplicate();

        Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> serializedDedupMaps = new HashMap<>();
        for (Map.Entry<ResourceID, Map<Integer, Map<Integer, HashMap<?, ?>>>> entry : dedupMaps.entrySet()) {
            ResourceID tmID = entry.getKey();
            Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> serializedDedupMapsForTM = new HashMap<>();

            for (Map.Entry<Integer, Map<Integer, HashMap<?, ?>>> entry2 : entry.getValue().entrySet()) {
                int partition = entry2.getKey();
                Map<Integer, HashMap<byte[], byte[]>> serializedDedupMapsForPartition = new HashMap<>();
                for (Map.Entry<Integer, HashMap<?, ?>> entry3 : entry2.getValue().entrySet()) {
                    int keyGroupIndex = entry3.getKey();
                    HashMap<byte[], byte[]> serializedDedupMapsForKeyGroup = serializeDedupMap(
                            entry3.getValue(), keySerializer, namespaceSerializer);
                    serializedDedupMapsForPartition.put(
                            keyGroupIndex,
                            serializedDedupMapsForKeyGroup);
                }
                serializedDedupMapsForTM.put(partition, serializedDedupMapsForPartition);
            }
            serializedDedupMaps.put(tmID, serializedDedupMapsForTM);
        }
        return serializedDedupMaps;
    }

    private HashMap<byte[], byte[]>[] serializeDedupMaps(HashMap<?, ?>[] dedupMaps) {
        InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                ((WindowOperator) mainOperator).getInternalTimerService() :
                ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

        TypeSerializer keySerializer = timerService.getKeySerializer().duplicate();
        TypeSerializer namespaceSerializer = timerService.getNamespaceSerializer().duplicate();

        HashMap<byte[], byte[]>[] serializedDedupMaps = new HashMap[dedupMaps.length];
        for (int i = 0; i < dedupMaps.length; i++) {
            serializedDedupMaps[i] = serializeDedupMap(dedupMaps[i], keySerializer, namespaceSerializer);
        }
        return serializedDedupMaps;
    }

    private HashMap<byte[], byte[]> serializeDedupMap(
            HashMap<?, ?> dedupMap, TypeSerializer keySerializer, TypeSerializer namespaceSerializer){
        HashMap<byte[], byte[]> serializedDedupMap = new HashMap<>();
        for (Map.Entry<?, ?> entry4 : dedupMap.entrySet()) {
            assert ((entry4.getKey() instanceof TimerHeapInternalTimer) &&
                    !(entry4.getValue() instanceof TimerHeapInternalTimer)) :
                    "The deduplication map is not of type TimerHeapInternalTimer";
            TimerHeapInternalTimer key = (TimerHeapInternalTimer) entry4.getKey();
            TimerHeapInternalTimer value = (TimerHeapInternalTimer) entry4.getValue();
            byte[] serializedKey = serializeTimer(
                    key,
                    keySerializer,
                    namespaceSerializer);
            byte[] serializedValue = serializeTimer(
                    value,
                    keySerializer,
                    namespaceSerializer);

            serializedDedupMap.put(serializedKey, serializedValue);
        }
        return serializedDedupMap;
    }

    private Map<ResourceID, Map<Integer, List<byte[]>>> serializeTriggers(
            Map<ResourceID, Map<Integer, List<HeapPriorityQueueElement>>> triggers) {
        InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                ((WindowOperator) mainOperator).getInternalTimerService() :
                ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

        TypeSerializer keySerializer = timerService.getKeySerializer().duplicate();
        TypeSerializer namespaceSerializer = timerService.getNamespaceSerializer().duplicate();

        Map<ResourceID, Map<Integer, List<byte[]>>> serializedTriggers = new HashMap<>();
        for (Map.Entry<ResourceID, Map<Integer, List<HeapPriorityQueueElement>>> entry : triggers.entrySet()) {
            ResourceID tmID = entry.getKey();
            Map<Integer, List<byte[]>> serializedTriggersForTM = new HashMap<>();
            for (Map.Entry<Integer, List<HeapPriorityQueueElement>> entry2 : entry.getValue().entrySet()) {
                int partition = entry2.getKey();
                List<byte[]> serializedTriggersList = new ArrayList<>();
                for (HeapPriorityQueueElement trigger : entry2.getValue()) {
                    assert trigger instanceof TimerHeapInternalTimer :
                            "The trigger is not of type TimerHeapInternalTimer";
                    byte[] serializedTimer = serializeTimer((TimerHeapInternalTimer) trigger,
                            keySerializer, namespaceSerializer);
                    serializedTriggersList.add(serializedTimer);
                }
                serializedTriggersForTM.put(partition, serializedTriggersList);
            }
            serializedTriggers.put(tmID, serializedTriggersForTM);
        }
        return serializedTriggers;
    }

    private byte[][] serializeTriggers(HeapPriorityQueueElement[] triggers) {
        InternalTimerServiceImpl timerService = mainOperator instanceof WindowOperator ?
                ((WindowOperator) mainOperator).getInternalTimerService() :
                ((KeyedCoProcessOperator) mainOperator).getInternalTimerService();

        TypeSerializer keySerializer = timerService.getKeySerializer().duplicate();
        TypeSerializer namespaceSerializer = timerService.getNamespaceSerializer().duplicate();

        byte[][] serializedTriggers = new byte[triggers.length][];
        for (int i = 0; i < triggers.length; i++) {
            assert triggers[i] instanceof TimerHeapInternalTimer :
                    "The trigger is not of type TimerHeapInternalTimer";
            serializedTriggers[i] = serializeTimer((TimerHeapInternalTimer) triggers[i],
                    keySerializer, namespaceSerializer);
        }
        return serializedTriggers;
    }

    private byte[] serializeTimer(TimerHeapInternalTimer timer, TypeSerializer keySerializer,
                                  TypeSerializer namespaceSerializer) {
        try {
            ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
            DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);
            outputView.writeLong(MathUtils.flipSignBit(timer.getTimestamp()));
            keySerializer.serialize(timer.getKey(), outputView);
            namespaceSerializer.serialize(timer.getNamespace(), outputView);
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize the timer");
        }
    }

    private void sendStateToTaskManagerPassiveQuery(ReconfigurationControlMessage controlMessage, String taskNameIdx, int taskIndex) {
        int numOfSenders = getNumOfTasks(controlMessage.numOfSenders, taskNameIdx,
                controlMessage.taskToResourceIDMap);
        int numOfReceivers = getNumOfTasks(controlMessage.numOfReceivers, taskNameIdx,
                controlMessage.taskToResourceIDMap);

        if (mainOperator instanceof AbstractStreamOperator){
            Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state;
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs =
                    new HashMap<>(controlMessage.newDOPActive);
            Map<Integer, String> stateMapDict = new HashMap<>(2);
            if (controlMessage.migrationType == MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE) {
                state = ((AbstractStreamOperator) mainOperator)
                        .getRuntimeContext()
                        .getEntriesInRangeForMigration(controlMessage.newDOPActive,
                                taskIndex,
                                controlMessage.maxParallelism,
                                controlMessage.rangeForMigration.start,
                                controlMessage.rangeForMigration.end,
                                controlMessage.rangeForMigration.start2,
                                controlMessage.rangeForMigration.end2,
                                controlMessage.partitionToResourceIDMap,
                                controlMessage.partitionToResourceIDMapPassive,
                                stateForOtherTMs, stateMapDict, controlMessage.activeChannelsOfActiveGroup);
            }
            else if (controlMessage.migrationType == MIGRATION_TYPE_FROM_PASSIVE_QUERY.ENTIRE_STATE) {
                state = ((AbstractStreamOperator) mainOperator)
                        .getRuntimeContext()
                        .getEntireStateForMigration(controlMessage.newDOPActive, taskIndex,
                                controlMessage.maxParallelism,
                                controlMessage.partitionToResourceIDMap,
                                controlMessage.partitionToResourceIDMapPassive, stateForOtherTMs,
                                stateMapDict, controlMessage.activeChannelsOfActiveGroup);
            }
            else {
                throw new RuntimeException("Migration type is NO_MIGRATION but a task is declared as "
                        + "sender of passive query");
            }
            taskManagerActions.sendStatePassiveQuery(taskIndex, state, stateForOtherTMs,
                    stateMapDict, numOfSenders, numOfReceivers, controlMessage.activeGroupId);
        }
        else {
            throw new RuntimeException("The target operator is not a stream operator." + taskNameIdx);
        }
    }

    private void sendStateToTaskManagerDownstream(ReconfigurationControlMessage controlMessage, String taskNameIdx, int taskIndex) {
        // TODO GroupShare this logic should be in the mailbox thread?
        if (mainOperator instanceof AbstractStreamOperator){
            // TODO GroupShare remove (this is here for debugging)
            //if (LOG.isDebugEnabled()) {
            // ((AbstractStreamOperator) mainOperator).getRuntimeContext().printState(taskNameIdx);
            //}

            ResourceID receiverTM = controlMessage.partitionToResourceIDMapDownstreamReceivers.get(taskIndex);
            boolean stateToBeMigratedToTheSameTM =
                    controlMessage.partitionToResourceIDMapDownstreamSenders.get(taskIndex)
                            .equals(receiverTM);

            Map<String, byte[][]> stateForOtherTM = new HashMap<>();
            Map<String, StateMap<?, ?, ?>[]> state = ((AbstractStreamOperator) mainOperator)
                    .getRuntimeContext().getEntriesForMigrationDownstream(
                            (!stateToBeMigratedToTheSameTM), stateForOtherTM);

            if (mainOperator instanceof WindowOperator) {
                HashMap<?, ?>[] dedupMaps = ((WindowOperator) mainOperator)
                        .getDeduplicationMapsForMigration();
                HeapPriorityQueueElement[] triggers = ((WindowOperator) mainOperator)
                        .getTriggersForMigration();
                int queueSize = ((WindowOperator) mainOperator).getQueueSize();

                // serialize dedupMaps and triggers
                HashMap<byte[], byte[]>[] serializedDedupMaps = stateToBeMigratedToTheSameTM ? null :
                        serializeDedupMaps(dedupMaps);
                byte[][] serializedTriggers = stateToBeMigratedToTheSameTM ? null :
                        serializeTriggers(triggers);

                taskManagerActions.sendStateOfDownstream(taskIndex, state, stateForOtherTM,
                        dedupMaps, serializedDedupMaps, triggers, serializedTriggers, queueSize,
                        controlMessage.downstreamSenders.size(), receiverTM,
                        controlMessage.multipleTMsInvolvedInMigrationDownstream,
                        controlMessage.numberOfSenderTMsInvolvedInMigrationDownstream,
                        controlMessage.activeGroupId);
            }
            else {
                // send the state to the task manager
                taskManagerActions.sendStateOfDownstream(taskIndex, state, stateForOtherTM,
                        controlMessage.downstreamSenders.size(), receiverTM,
                        controlMessage.multipleTMsInvolvedInMigrationDownstream,
                        controlMessage.numberOfSenderTMsInvolvedInMigrationDownstream,
                        controlMessage.activeGroupId);
            }
        }
        else {
            LOG.info("The target operator is not a stream operator.");
        }
    }

    private void sendControlReceiver(ReconfigurationControlMessage controlMessage, String taskNameIdx, int taskIndex,
                                     boolean isDownstream) {
        // a receiver must communicate to the taskManager its executionId
        if (isDownstream) {
            taskManagerActions.sendDownstreamReceiverExecutionAttemptID(taskIndex,
                    getEnvironment().getExecutionId(),
                    controlMessage.downstreamReceivers.size(), controlMessage.activeGroupId);
        }
        else {
            assert controlMessage.numOfSenders != null;
            assert controlMessage.numOfReceivers != null;
            int numOfSenders = getNumOfTasks(controlMessage.numOfSenders, taskNameIdx,
                    controlMessage.taskToResourceIDMap);
            int numOfReceivers = getNumOfTasks(controlMessage.numOfReceivers, taskNameIdx,
                    controlMessage.taskToResourceIDMap);

            taskManagerActions.sendReceiverExecutionAttemptID(taskIndex,
                    getEnvironment().getExecutionId(), numOfSenders,
                    numOfReceivers, controlMessage.multipleTMsInvolvedInMigration,
                    controlMessage.numberOfSenderTMsInvolvedInMigration,
                    controlMessage.activeGroupId);
        }
        this.isWaitingForInMemState = true;
        this.isWaitingForSerializedState = controlMessage.multipleTMsInvolvedInMigration;
        if (controlMessage.multipleTMsInvolvedInMigration && controlMessage.numOfSenders.size() == 1) {
            if (controlMessage.numOfSenders.containsKey(controlMessage.taskToResourceIDMap.get(taskNameIdx))){
                this.isWaitingForSerializedState = false;
            }
        }
        this.pendingControlMessage = controlMessage;
    }

    private void tryToSendPendingControlMessage() {
        if (isWaitingForInMemState || isWaitingForSerializedState) {
            LOG.debug(GROUP_SHARE, "Waiting for state migration to finish "
                    + isWaitingForInMemState + " " + isWaitingForSerializedState + " "
                    + getEnvironment().getTaskInfo().getTaskName()
                    + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
            return;
        }
        if (isRunning) {
            assert (pendingControlMessage != null);
            sendControlMessageDownstream(
                    pendingControlMessage,
                    false, changeDOPafterPendingControlMessage);
        }
        pendingControlMessage = null;
        changeDOPafterPendingControlMessage = false;
        LOG.debug(GROUP_SHARE, "Pending control message sent "
                + getEnvironment().getTaskInfo().getTaskName()
                + "-" + getEnvironment().getTaskInfo().getIndexOfThisSubtask());
    }

    private List<Integer> getNewActiveChannels(ReconfigurationControlMessage controlMessage) {
        List<Integer> newActiveChannels = null;
        if ((controlMessage.isGroupActive)) {
            newActiveChannels = controlMessage.activeChannelsOfActiveGroup;
        }
        if (!controlMessage.isGroupActive && !controlMessage.share) {
            newActiveChannels = controlMessage.activeChannelsOfPassiveGroup;
        }
        return newActiveChannels;
    }

    @Override
    public void sendLastTupleData(Map<Integer, ReconfigurableSourceData> lastTupleDataMap) {
        assert (mainOperator instanceof AbstractUdfStreamOperator &&
                (((AbstractUdfStreamOperator)mainOperator).getUserFunction()
                        instanceof RichParallelSourceFunction));
        // when the passive query receives the last tuple, it must forward downstream the control message
        // so that the state in the downstream tasks can be migrated to the active query
        assert pendingControlMessage != null;
        sendControlMessageDownstream(pendingControlMessage, false, false);
        AbstractUdfStreamOperator udfOperator = (AbstractUdfStreamOperator) mainOperator;
        ((RichParallelSourceFunction) udfOperator.getUserFunction())
                .requestDisconnectionFromDriver(lastTupleDataMap);
    }

    @Override
    public void modifySourceConnection(String host, int port) {
        assert (mainOperator instanceof AbstractUdfStreamOperator &&
                (((AbstractUdfStreamOperator)mainOperator).getUserFunction()
                        instanceof RichParallelSourceFunction));
        AbstractUdfStreamOperator udfOperator = (AbstractUdfStreamOperator) mainOperator;
        ((RichParallelSourceFunction) udfOperator.getUserFunction())
                .modifySourceConnection(host, port);

        // when the passive query receives the new source connection info, it must forward downstream
        // the control message so that the state can be migrated from the downstream operators of
        // the active query to the downstream of the passive
        assert pendingControlMessage != null;
        sendControlMessageDownstream(pendingControlMessage, false, false);
    }

    public void sendPendingControlMessgaeDownstream() {
        if (pendingControlMessage != null) {
            TaskInfo info = getEnvironment().getTaskInfo();
            int taskIndex = info.getIndexOfThisSubtask();
            sendControlMessageDownstream(
                    pendingControlMessage, false,
                    changeDOPafterPendingControlMessage);
            changeDOPafterPendingControlMessage = false;
            pendingControlMessage = null;
        }
    }

    private void sendControlMessageDownstream(
            ReconfigurationControlMessage controlMessage, boolean sendLastTupleBeforeEmittingCtrlMsg,
            boolean changeDOP) {
        TaskInfo info = getEnvironment().getTaskInfo();
        String name = info.getTaskName();
        int subtaskIdx = info.getIndexOfThisSubtask();
        String taskNameIdx = name + "-" + subtaskIdx;
        LOG.debug(GROUP_SHARE, "Sending control message downstream from " + taskNameIdx + " with changeDOP = " + changeDOP);
        try{
            CompletableFuture<Void> f = new CompletableFuture<>();
            mainMailboxExecutor.execute(() -> {

                if (sendLastTupleBeforeEmittingCtrlMsg) {
                    AbstractUdfStreamOperator udfOperator = (AbstractUdfStreamOperator) mainOperator;
                    // get current state
                    ReconfigurableSourceData lastTupleData =
                            ((RichParallelSourceFunction) udfOperator.getUserFunction()).getData();
                    ResourceID resourceID = controlMessage.sourceToResourceIDMap.get(taskNameIdx);
                    // send it to the task manager
                    taskManagerActions.sendLastTupleData(
                            lastTupleData.getDriverSubTaskIdx(), lastTupleData,
                            controlMessage.sourcesOfActiveQuery.get(resourceID).size(),
                            controlMessage.sourcesOfActiveQuery.size(), controlMessage.activeGroupId);
                }
                //controlMessage.callback().accept(new Object[]{jobVId, subtaskIdx, name});
                CheckpointBarrier barrier = new CheckpointBarrier(ControlMessage.FixedEpochNumber,
                        -1, CheckpointOptions.forCheckpointWithDefaultLocation());
                barrier.setMessage(controlMessage);
                // TODO GroupShare should I put here controlMessage.maxDOP()?
                boolean changeDOPAfterBarrier = changeDOP;
                List<Integer> newActiveChannels = getNewActiveChannels(controlMessage);
                if (changeDOP) {
                    int oldDOP  = controlMessage.isGroupActive ? controlMessage.oldDOPActive :
                            controlMessage.oldDOPPassive;
                    int newDOP = controlMessage.isGroupActive ? controlMessage.newDOPActive :
                            controlMessage.newDOPPassive;
                    if (newDOP > oldDOP) {
                        changeDOP(newDOP, newActiveChannels);
                        changeDOPAfterBarrier = false; // DOP already changed
                    }
                }
                operatorChain.broadcastEvent(barrier, false,
                        controlMessage.MCS.get(taskNameIdx).stream()
                                .map(s -> s.substring(0,s.lastIndexOf('-')))
                                .collect(Collectors.toSet()));
                if (changeDOPAfterBarrier) {
                    int newDOP = controlMessage.isGroupActive ? controlMessage.newDOPActive :
                            controlMessage.newDOPPassive;
                    changeDOP(newDOP, newActiveChannels);
                }
                setSplitMonitoring(taskNameIdx, controlMessage.enableSplitMonitoring);
                f.complete(null);
            },"control", controlMessage, f);
            if(!mailboxProcessor.isMailboxThread()){
                f.get();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void sendMonitoringControlMessageDownstream(
            ControlMessage controlMessage) {
        TaskInfo info = getEnvironment().getTaskInfo();
        String name = info.getTaskName();
        int subtaskIdx = info.getIndexOfThisSubtask();
        String taskNameIdx = name + "-" + subtaskIdx;
        LOG.debug(GROUP_SHARE, "Sending monitoring control message downstream from " + taskNameIdx);
        try{
            CompletableFuture<Void> f = new CompletableFuture<>();
            mainMailboxExecutor.execute(() -> {
                CheckpointBarrier barrier = new CheckpointBarrier(ControlMessage.FixedEpochNumber,
                        -1, CheckpointOptions.forCheckpointWithDefaultLocation());
                barrier.setMessage(controlMessage);
                operatorChain.broadcastEvent(barrier, false,
                        controlMessage.MCS.get(taskNameIdx).stream()
                                .map(s -> s.substring(0,s.lastIndexOf('-')))
                                .collect(Collectors.toSet()));
                f.complete(null);
            },"control", controlMessage, f);
            if(!mailboxProcessor.isMailboxThread()){
                f.get();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void setSplitMonitoring(String taskName, boolean monitoringEnabled) {
        if (taskName.contains("StatefulJoin")){
            JoinMonitor joinMonitor = getJoinMonitor();
            if (joinMonitor != null) {
                if (monitoringEnabled) {
                    joinMonitor.enableMonitoringSplit();
                } else {
                    joinMonitor.disableMonitoringSplit();
                }
            }
        }
    }

    public double getIdlePercentage() {
        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        return ioMetrics.getIdleTimeMsPerSecond().getValue() / 1000.0;
    }

    private void changeDOP(int newDOPVal, List<Integer> newActiveChannels) {
        if (recordWriter instanceof SingleRecordWriter) {
            recordWriter.getRecordWriter(0).setNumberOfChannels(newDOPVal, newActiveChannels);
        }
        else if (recordWriter instanceof MultipleRecordWriters){
            for (int i=0; i < ((MultipleRecordWriters) recordWriter).getNumberOfRecordWriters() ; i++) {
                ((MultipleRecordWriters) recordWriter)
                        .getRecordWriter(i)
                        .setNumberOfChannels(newDOPVal, newActiveChannels);
            }
        }
    }

    private int getNumOfTasks(HashMap<ResourceID, HashMap<String, Integer>> numOfTasks,
                              String taskNameIdx, HashMap<String, ResourceID> taskToResourceIDMap) {
        assert numOfTasks != null;
        HashMap<String, Integer> numOfTasksFromThisTM = numOfTasks
                .get(taskToResourceIDMap.get(taskNameIdx));
        if (numOfTasksFromThisTM == null) {
            return 0;
        }
        for (Map.Entry<String, Integer> entry : numOfTasksFromThisTM.entrySet()) {
            if (taskNameIdx.startsWith(entry.getKey())){
                return entry.getValue();
            }
        }
        throw new RuntimeException("The number of tasks for " + taskNameIdx + " is not set");
    }

    @Override
    public TaskUtilizationStats sendMonitoringMsg() {
        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        long idleTime = ioMetrics.getIdleTimeMsPerSecond().getValue();
        double busyTime = ioMetrics.getBusyTimePerSecond();
        long backpressuredTime = ioMetrics.getBackPressuredTimePerSecond().getValue();
        return new TaskUtilizationStats(idleTime, backpressuredTime, busyTime);
    }

    @Override
    public double sendThroughputMonitoringMsg() {
        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        return ioMetrics.getNumRecordsOutPerSecond();
    }

    @Override
    public DataDistrSplitStats sendSplitPhaseMonitoringMsg() {
        JoinMonitor joinMonitor = getJoinMonitor();
        return joinMonitor.getSplitMonitoringStats();
    }

    @Override
    public void pause() {
        System.out.println("StreamTask receives pause! current thread = "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
        mainMailboxExecutor.execute(() -> {},"pause");
    }

    @Override
    public void resume(){
        mainMailboxExecutor.execute(() -> {}, "resume");
    }

    @Override
    public CompletableFuture<?> shutdown() {
        timer.cancel();
        return FutureUtils.completedVoidFuture();
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }

    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------

    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    protected void cleanup() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        if (isWaitingForInMemState || isWaitingForSerializedState) {
            // we must not process any further input until state migration is completed
            return;
        }
        InputStatus status = inputProcessor.processInput();
        if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
            return;
        }
        if (status == InputStatus.END_OF_INPUT) {
            controller.allActionsCompleted();
            return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        TimerGauge timer;
        CompletableFuture<?> resumeFuture;
        if (!recordWriter.isAvailable()) {
            timer = ioMetrics.getBackPressuredTimePerSecond();
            resumeFuture = recordWriter.getAvailableFuture();
        } else {
            timer = ioMetrics.getIdleTimeMsPerSecond();
            resumeFuture = inputProcessor.getAvailableFuture();
        }
        assertNoException(
                resumeFuture.thenRun(
                        new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
    }

    private void resetSynchronousSavepointId(long id, boolean succeeded) {
        if (!succeeded && activeSyncSavepointId != null && activeSyncSavepointId == id) {
            // allow to process further EndOfPartition events
            activeSyncSavepointId = null;
            operatorChain.setIgnoreEndOfInput(false);
        }
        syncSavepointId = null;
    }

    private void setSynchronousSavepointId(long checkpointId, boolean ignoreEndOfInput) {
        checkState(
                syncSavepointId == null,
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        syncSavepointId = checkpointId;
        activeSyncSavepointId = checkpointId;
        operatorChain.setIgnoreEndOfInput(ignoreEndOfInput);
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        return syncSavepointId != null ? OptionalLong.of(syncSavepointId) : OptionalLong.empty();
    }

    private boolean isSynchronousSavepointId(long checkpointId) {
        return syncSavepointId != null && syncSavepointId == checkpointId;
    }

    private void runSynchronousSavepointMailboxLoop() throws Exception {
        assert syncSavepointId != null;

        MailboxExecutor mailboxExecutor =
                mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY);

        while (!canceled && syncSavepointId != null) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void advanceToEndOfEventTime() throws Exception {}

    /**
     * Instructs the task to go through its normal termination routine, i.e. exit the run-loop and
     * call {@link StreamOperator#close()} and {@link StreamOperator#dispose()} on its operators.
     *
     * <p>This is used by the source task to get out of the run-loop when the job is stopped with a
     * savepoint.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void finishTask() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer() {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return ((OperatorMetricGroup) streamOperator.getMetricGroup())
                    .getIOMetricGroup()
                    .getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        runWithCleanUpOnFail(this::executeRestore);
    }

    void executeRestore() throws Exception {
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        disposedOperators = false;
        LOG.debug("Initializing {}.", getName());

        operatorChain = new OperatorChain<>(this, recordWriter);
        mainOperator = operatorChain.getMainOperator();

        // task specific initialization
        init();

        // save the work of reloading state, etc, if the task is already canceled
        ensureNotCanceled();

        // -------- Invoke --------
        LOG.debug("Invoking {}", getName());

        // we need to make sure that any triggers scheduled in open() cannot be
        // executed before all operators are opened
        CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);

        // Run mailbox until all gates will be recovered.
        mailboxProcessor.runMailboxLoop();

        ensureNotCanceled();

        checkState(
                allGatesRecoveredFuture.isDone(),
                "Mailbox loop interrupted before recovery was finished.");

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        int i = 0;
        for (InputGate inputGate : inputGates) {
            final int fi = i;
            mainMailboxExecutor.execute(
                    inputGate::requestPartitions,
                    "Input gate request partitions" + fi);
            i++;
        }

        isRunning = true;
    }

    private CompletableFuture<Void> restoreGates() throws Exception {
        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }

    @Override
    public final void invoke() throws Exception {
        runWithCleanUpOnFail(this::executeInvoke);

        cleanUpInvoke();
    }

    private void executeInvoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        if (!isRunning) {
            LOG.debug("Restoring during invoke will be called.");
            executeRestore();
        }

        // final check to exit early before starting to run
        ensureNotCanceled();


        // let the task do its work
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        ensureNotCanceled();

        afterInvoke();
    }

    private void runWithCleanUpOnFail(RunnableWithException run) throws Exception {
        try {
            run.run();
        } catch (Throwable invokeException) {
            failing = !canceled;
            try {
                if (!canceled) {
                    try {
                        cancelTask();
                    } catch (Throwable ex) {
                        invokeException = firstOrSuppressed(ex, invokeException);
                    }
                }

                cleanUpInvoke();
            }
            // TODO: investigate why Throwable instead of Exception is used here.
            catch (Throwable cleanUpException) {
                rethrowException(firstOrSuppressed(cleanUpException, invokeException));
            }

            rethrowException(invokeException);
        }
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        final CompletableFuture<Void> timersFinishedFuture = new CompletableFuture<>();

        // close all operators in a chain effect way
        operatorChain.closeOperators(actionExecutor);

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        actionExecutor.runThrowing(
                () -> {

                    // make sure no new timers can come
                    FutureUtils.forward(timerService.quiesce(), timersFinishedFuture);

                    // let mailbox execution reject all new letters from this point
                    mailboxProcessor.prepareClose();

                    // only set the StreamTask to not running after all operators have been closed!
                    // See FLINK-7430
                    isRunning = false;
                });
        // processes the remaining mails; no new mails can be enqueued
        mailboxProcessor.drain();

        // make sure all timers finish
        timersFinishedFuture.get();

        LOG.debug("Closed operators for task {}", getName());

        // make sure all buffered data is flushed
        operatorChain.flushOutputs();

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        disposeAllOperators();
    }

    protected void cleanUpInvoke() throws Exception {
        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        isRunning = false;

        // Now that we are outside the user code, we do not want to be interrupted further
        // upon cancellation. The shutdown logic below needs to make sure it does not issue calls
        // that block and stall shutdown.
        // Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
        // process) as a backup in case some shutdown procedure blocks outside our control.
        setShouldInterruptOnCancel(false);

        // clear any previously issued interrupt for a more graceful shutdown
        Thread.interrupted();

        // stop all timers and threads
        Exception suppressedException =
                runAndSuppressThrowable(this::tryShutdownTimerService, null);

        // stop all asynchronous checkpoint threads
        suppressedException = runAndSuppressThrowable(cancelables::close, suppressedException);
        suppressedException =
                runAndSuppressThrowable(this::shutdownAsyncThreads, suppressedException);

        // we must! perform this cleanup
        suppressedException = runAndSuppressThrowable(this::cleanup, suppressedException);

        // if the operators were not disposed before, do a hard dispose
        suppressedException =
                runAndSuppressThrowable(this::disposeAllOperators, suppressedException);

        // release the output resources. this method should never fail.
        suppressedException =
                runAndSuppressThrowable(this::releaseOutputResources, suppressedException);

        suppressedException =
                runAndSuppressThrowable(channelIOExecutor::shutdown, suppressedException);

        suppressedException = runAndSuppressThrowable(mailboxProcessor::close, suppressedException);

        if (suppressedException == null) {
            terminationFuture.complete(null);
        } else {
            terminationFuture.completeExceptionally(suppressedException);
            throw suppressedException;
        }

    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final Future<Void> cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
        return terminationFuture;
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }

    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            actionExecutor.run(() -> operatorChain.releaseOutputs());
        } else {
            // failed to allocate operatorChain, clean up record writers
            recordWriter.close();
        }
    }

    private Exception runAndSuppressThrowable(
            ThrowingRunnable<?> runnable, @Nullable Exception originalException) {
        try {
            runnable.run();
        } catch (Throwable t) {
            // TODO: investigate why Throwable instead of Exception is used here.
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            return firstOrSuppressed(e, originalException);
        }

        return originalException;
    }

    /**
     * Execute @link StreamOperator#dispose()} of each operator in the chain of this {@link
     * StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
     */
    private void disposeAllOperators() throws Exception {
        if (operatorChain != null && !disposedOperators) {
            Exception disposalException = null;
            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                StreamOperator<?> operator = operatorWrapper.getStreamOperator();
                try {
                    operator.dispose();
                } catch (Exception e) {
                    disposalException = firstOrSuppressed(e, disposalException);
                }
            }
            disposedOperators = true;
            if (disposalException != null) {
                throw disposalException;
            }
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    public StreamStatusMaintainer getStreamStatusMaintainer() {
        return operatorChain;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public Future<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    latestAsyncCheckpointStartDelayNanos =
                            1_000_000
                                    * Math.max(
                                            0,
                                            System.currentTimeMillis()
                                                    - checkpointMetaData.getTimestamp());
                    result.complete(triggerCheckpoint(checkpointMetaData, checkpointOptions));
                },
                "checkpoint",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            // No alignment if we inject a checkpoint
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L);

            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics)) {
                if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
                    runSynchronousSavepointMailboxLoop();
                }
            }
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        resetSynchronousSavepointId(checkpointId, false);
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        System.out.println(
                String.format("Starting checkpoint (%d) %s on task %s",
                checkpointMetaData.getCheckpointId(),
                checkpointOptions.getCheckpointType(),
                getName()));

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        if (checkpointOptions.getCheckpointType().isSynchronous()) {
                            setSynchronousSavepointId(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointOptions.getCheckpointType().shouldIgnoreEndOfInput());

                            if (checkpointOptions.getCheckpointType().shouldAdvanceToEndOfTime()) {
                                advanceToEndOfEventTime();
                            }
                        } else if (activeSyncSavepointId != null
                                && activeSyncSavepointId < checkpointMetaData.getCheckpointId()) {
                            activeSyncSavepointId = null;
                            operatorChain.setIgnoreEndOfInput(false);
                        }

                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        System.out.println("notify chkpt completed received!");
        return notifyCheckpointOperation(
                () -> notifyCheckpointComplete(checkpointId),
                "checkpoint complete", checkpointId);
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () -> {
                    resetSynchronousSavepointId(checkpointId, false);
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                "checkpoint aborted", checkpointId);
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description, Object... params) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description, params);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning && isSynchronousSavepointId(checkpointId)) {
            finishTask();
            // Reset to "notify" the internal synchronous savepoint mailbox loop.
            resetSynchronousSavepointId(checkpointId, true);
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    private StateBackend createStateBackend() throws Exception {
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());

        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        final Path savepointDir = configuration.getSavepointDir(getUserCodeClassLoader());

        return CheckpointStorageLoader.load(
                fromApplication,
                savepointDir,
                backend,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRunning) {
            // only fail if the task is still running
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------

    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    static class StreamTaskAsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        if (recordWrites.size() == 1) {
            return new SingleRecordWriter<>(recordWrites.get(0));
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
        } else {
            return new MultipleRecordWriters<>(recordWrites);
        }
    }

    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        List<StreamEdge> outEdgesInOrder =
                configuration.getOutEdgesInOrder(
                        environment.getUserCodeClassLoader().asClassLoader());

        for (int i = 0; i < outEdgesInOrder.size(); i++) {
            StreamEdge edge = outEdgesInOrder.get(i);
            recordWriters.add(
                    createRecordWriter(
                            edge,
                            i,
                            environment,
                            environment.getTaskInfo().getTaskName(),
                            edge.getBufferTimeout()));
        }
        return recordWriters;
    }

    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            StreamEdge edge,
            int outputIndex,
            Environment environment,
            String taskName,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) edge.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskName);

        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskName)
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback");
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    private static class ResumeWrapper implements Runnable {
        private final Suspension suspendedDefaultAction;
        private final TimerGauge timer;

        public ResumeWrapper(Suspension suspendedDefaultAction, TimerGauge timer) {
            this.suspendedDefaultAction = suspendedDefaultAction;
            timer.markStart();
            this.timer = timer;
        }

        @Override
        public void run() {
            timer.markEnd();
            suspendedDefaultAction.resume();
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }
}
