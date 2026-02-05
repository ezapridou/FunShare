/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import net.michaelkoepf.spegauge.api.sut.DataDistrSplitStats;
import net.michaelkoepf.spegauge.api.sut.FilterDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.JoinDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.ReconfigurableSourceData;

import org.apache.flink.extensions.controller.ControlMessage;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.extensions.controller.ReconfigurationControlMessage;
import org.apache.flink.extensions.controller.StartMonitoringDataDistrControlMessage;
import org.apache.flink.extensions.controller.StopMonitoringDataDistrControlMessage;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointType.PostCheckpointAction;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.extensions.controller.TaskUtilizationStats;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotActions;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlot;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ThreadInfo;
import java.net.InetSocketAddress;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    public static final String TASK_MANAGER_NAME = "taskmanager";

    /** The access to the leader election and retrieval services. */
    private final HighAvailabilityServices haServices;

    private final TaskManagerServices taskExecutorServices;

    /** The task manager configuration. */
    private final TaskManagerConfiguration taskManagerConfiguration;

    /** The fatal error handler to use in case of a fatal error. */
    private final FatalErrorHandler fatalErrorHandler;

    private final BlobCacheService blobCacheService;

    private final LibraryCacheManager libraryCacheManager;

    /** The address to metric query service on this Task Manager. */
    @Nullable private final String metricQueryServiceAddress;

    // --------- TaskManager services --------

    /** The connection information of this task manager. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final TaskManagerMetricGroup taskManagerMetricGroup;

    /** The state manager for this task, providing state managers per slot. */
    private final TaskExecutorLocalStateStoresManager localStateStoresManager;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The network component in the task manager. */
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    /** The kvState registration service in the task manager. */
    private final KvStateService kvStateService;

    private final Executor ioExecutor;

    // --------- task slot allocation table -----------

    private final TaskSlotTable<Task> taskSlotTable;

    private final Map<JobID, UUID> currentSlotOfferPerJob = new HashMap<>();

    private final JobTable jobTable;

    private final JobLeaderService jobLeaderService;

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // ------------------------------------------------------------------------

    private final HardwareDescription hardwareDescription;

    private final TaskExecutorMemoryConfiguration memoryConfiguration;

    private FileCache fileCache;

    /** The heartbeat manager for job manager in the task manager. */
    private final HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            jobManagerHeartbeatManager;

    /** The heartbeat manager for resource manager in the task manager. */
    private final HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            resourceManagerHeartbeatManager;

    private final TaskExecutorPartitionTracker partitionTracker;

    // --------- resource manager --------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    @Nullable private TaskExecutorToResourceManagerConnection resourceManagerConnection;

    @Nullable private UUID currentRegistrationTimeoutId;

    private Map<JobID, Collection<CompletableFuture<ExecutionState>>>
            taskResultPartitionCleanupFuturesPerJob = new HashMap<>(8);

    private final ThreadInfoSampleService threadInfoSampleService;

    // --------- reconfiguration --------
    // initial capacity for data structures that can reach maximum size of MAX_PARALLELISM
    private final int INITIAL_CAPACITY = 16;
    private class ReconfData {
        // Map<destinationPartitionID, Map<KVStateName, Map<KeyGroupIndex, keyGroupsToBeMigrated>>>
        private final Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> stateToBeMigrated;
        // Map<resourceIDOfReceiverTM, Map<partitionID, Map<StateNameId, Map<KeyGroupID, Set<serializedState>>>>>
        private final Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs;
        private Map<Integer, String> stateNameDict;
        private final Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> stateToBeMigratedPassiveQuery;
        private final Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMsPassiveQuery;
        // Map<destinationPartitionID, Map<KeyGroupIndex, keyGroupsToBeMigrated>>
        private final Map<Integer, Map<Integer, HashMap<?, ?>>> dedupMapsToBeMigrated;
        private final Map<Integer, List<HeapPriorityQueueElement>> triggersToBeMigrated;
        private final Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> deduplicationMapsForOtherTMs;
        private final Map<ResourceID, Map<Integer, List<byte[]>>> triggersForOtherTMs;
        private final List<Integer> senders;
        private final Map<Integer, ExecutionAttemptID> receivers;
        private int totalNumOfSenders = 0;
        private int totalNumOfReceivers = 0;
        private boolean multipleTMsInvolvedInReconfiguration = false;
        private int numOfSenderTMs = 0;

        public ReconfData() {
            this.senders = new ArrayList<>(INITIAL_CAPACITY);
            this.receivers = new HashMap<>(INITIAL_CAPACITY);
            this.stateToBeMigrated = new HashMap<>(INITIAL_CAPACITY);
            this.stateToBeMigratedPassiveQuery = new HashMap<>(INITIAL_CAPACITY);
            this.stateForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.stateForOtherTMsPassiveQuery = new HashMap<>(INITIAL_CAPACITY);
            this.stateNameDict = new HashMap<>(2);
            this.dedupMapsToBeMigrated = new HashMap<>(INITIAL_CAPACITY);
            this.triggersToBeMigrated = new HashMap<>(INITIAL_CAPACITY);
            this.deduplicationMapsForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.triggersForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
        }
    }

    private class ReconfDataDownstream {
        private boolean multipleTMsInvolvedInReconfigurationDownstream = false;
        private int numOfSenderTMsForDownstream = 0;
        private final Map<Integer, ExecutionAttemptID> downstreamReceivers;
        // Map<destinationPartitionID, Map<KVStateName, StateMap<?, ?, ?>[]>>
        private final Map<Integer, Map<String, StateMap<?, ?, ?>[]>> stateOfDownstream;
        private final Map<ResourceID, Map<Integer, Map<String, byte[][]>>> stateDownstreamForOtherTMs;
        // Map<destinationPartitionID, deduplicationMaps>
        private final Map<Integer, HashMap<?, ?>[]> dedupMapsOfDownstream;
        private final Map<ResourceID, Map<Integer, HashMap<byte[], byte[]>[]>> dedupMapsOfDownstreamForOtherTMs;
        // Map<destinationPartitionID, triggers>
        private final Map<Integer, TriggersOfDownstream> triggersOfDownstream;
        private final Map<ResourceID, Map<Integer, byte[][]>> triggersOfDownstreamForOtherTMs;
        private final Map<ResourceID, Map<Integer, Integer>> queueSizeOfDownstreamForOtherTMs;
        private int totalNumOfDownstreamSenders= 0;
        private int totalNumOfDownstreamReceivers = 0;

        public ReconfDataDownstream() {
            this.stateOfDownstream = new HashMap<>(INITIAL_CAPACITY);
            this.dedupMapsOfDownstream = new HashMap<>(INITIAL_CAPACITY);
            this.triggersOfDownstream = new HashMap<>(INITIAL_CAPACITY);
            this.stateDownstreamForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.dedupMapsOfDownstreamForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.triggersOfDownstreamForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.queueSizeOfDownstreamForOtherTMs = new HashMap<>(INITIAL_CAPACITY);
            this.downstreamReceivers = new HashMap<>(INITIAL_CAPACITY);
        }
    }

    private class ReconfDataSource {
        private Map<Integer, ReconfigurableSourceData> lastTupleDataMap;
        private final Set<ExecutionAttemptID> sourceTasksOfPasiveQuery;
        private int totalNumOfSourcesActive = 0;
        private int totalNumOfSourcesPassive = 0;
        private int numOfTMsWithSources = 0;
        private String hostOfNewDriver = "";
        private int portOfNewDriver = 0;

        public ReconfDataSource() {
            this.lastTupleDataMap = new HashMap<>(INITIAL_CAPACITY);
            this.sourceTasksOfPasiveQuery = new HashSet<>(INITIAL_CAPACITY);
        }
    }

    private class PendingSerializedState {
        private Map<Integer, Map<Integer, Map<Integer, byte[]>>> state;
        private Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> dedupMaps;
        private Map<Integer, List<byte[]>> triggers;
        private Map<Integer, Map<Integer, Map<Integer, byte[]>>> statePassiveQuery;
        private Map<Integer, String> stateNameDict;

        public PendingSerializedState(Map<Integer, Map<Integer, Map<Integer, byte[]>>> state,
                                      Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> dedupMaps,
                                      Map<Integer, List<byte[]>> triggers,
                                      Map<Integer, Map<Integer, Map<Integer, byte[]>>> statePassiveQuery,
                                      Map<Integer, String> stateNamesDict) {
            this.state = state;
            this.dedupMaps = dedupMaps;
            this.triggers = triggers;
            this.statePassiveQuery = statePassiveQuery;
            this.stateNameDict = stateNamesDict;
        }
    }

    private HashMap<Integer, PendingSerializedState> pendingSerializedStatePerActiveGroup;

    private ResourceID resourceIDOfThisTM = null;
    private final int OFFSET_FOR_SENDER_ID_PASSIVE_QUERY = 10000;

    private final HashMap<Integer, ReconfData> reconfDataPerActiveGroup;
    private final List<ReconfData> reconfDataPool;

    private final HashMap<Integer, ReconfDataDownstream> reconfDataPerActiveGroupDownstream;
    private final List<ReconfDataDownstream> reconfDataPoolDownstream;

    private final HashMap<Integer, ReconfDataSource> reconfDataPerActiveGroupSource;
    private final List<ReconfDataSource> reconfDataPoolSource;

    public TaskExecutor(
            RpcService rpcService,
            TaskManagerConfiguration taskManagerConfiguration,
            HighAvailabilityServices haServices,
            TaskManagerServices taskExecutorServices,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            HeartbeatServices heartbeatServices,
            TaskManagerMetricGroup taskManagerMetricGroup,
            @Nullable String metricQueryServiceAddress,
            BlobCacheService blobCacheService,
            FatalErrorHandler fatalErrorHandler,
            TaskExecutorPartitionTracker partitionTracker) {

        super(rpcService, AkkaRpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

        checkArgument(
                taskManagerConfiguration.getNumberSlots() > 0,
                "The number of slots has to be larger than 0.");

        this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
        this.taskExecutorServices = checkNotNull(taskExecutorServices);
        this.haServices = checkNotNull(haServices);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.partitionTracker = partitionTracker;
        this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
        this.blobCacheService = checkNotNull(blobCacheService);
        this.metricQueryServiceAddress = metricQueryServiceAddress;
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.libraryCacheManager = taskExecutorServices.getLibraryCacheManager();
        this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
        this.jobTable = taskExecutorServices.getJobTable();
        this.jobLeaderService = taskExecutorServices.getJobLeaderService();
        this.unresolvedTaskManagerLocation =
                taskExecutorServices.getUnresolvedTaskManagerLocation();
        this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
        this.shuffleEnvironment = taskExecutorServices.getShuffleEnvironment();
        this.kvStateService = taskExecutorServices.getKvStateService();
        this.ioExecutor = taskExecutorServices.getIOExecutor();
        this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

        this.hardwareDescription =
                HardwareDescription.extractFromSystem(taskExecutorServices.getManagedMemorySize());
        this.memoryConfiguration =
                TaskExecutorMemoryConfiguration.create(taskManagerConfiguration.getConfiguration());

        this.resourceManagerAddress = null;
        this.resourceManagerConnection = null;
        this.currentRegistrationTimeoutId = null;

        final ResourceID resourceId =
                taskExecutorServices.getUnresolvedTaskManagerLocation().getResourceID();
        this.jobManagerHeartbeatManager =
                createJobManagerHeartbeatManager(heartbeatServices, resourceId);
        this.resourceManagerHeartbeatManager =
                createResourceManagerHeartbeatManager(heartbeatServices, resourceId);

        ExecutorThreadFactory sampleThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("flink-thread-info-sampler")
                        .build();
        ScheduledExecutorService sampleExecutor =
                Executors.newSingleThreadScheduledExecutor(sampleThreadFactory);
        this.threadInfoSampleService = new ThreadInfoSampleService(sampleExecutor);

        this.reconfDataPerActiveGroup = new HashMap<>(8);
        this.reconfDataPool = new ArrayList<>(8);
        for (int i = 0; i < 16; i++) {
            this.reconfDataPool.add(new ReconfData());
        }

        this.reconfDataPerActiveGroupDownstream = new HashMap<>(8);
        this.reconfDataPoolDownstream = new ArrayList<>(8);
        for (int i = 0; i < 16; i++) {
            this.reconfDataPoolDownstream.add(new ReconfDataDownstream());
        }

        this.reconfDataPerActiveGroupSource = new HashMap<>(8);
        this.reconfDataPoolSource = new ArrayList<>(8);
        for (int i = 0; i < 16; i++) {
            this.reconfDataPoolSource.add(new ReconfDataSource());
        }

        this.pendingSerializedStatePerActiveGroup = new HashMap<>(8);
    }

    private HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            createResourceManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            createJobManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new JobManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return CompletableFuture.completedFuture(
                shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
                    if (logDir != null) {
                        final File[] logFiles = new File(logDir).listFiles();

                        if (logFiles == null) {
                            throw new CompletionException(
                                    new FlinkException(
                                            String.format(
                                                    "There isn't a log file in TaskExecutor’s log dir %s.",
                                                    logDir)));
                        }

                        return Arrays.stream(logFiles)
                                .filter(File::isFile)
                                .map(logFile -> new LogInfo(logFile.getName(), logFile.length()))
                                .collect(Collectors.toList());
                    }
                    return Collections.emptyList();
                },
                ioExecutor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    // ------------------------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            startTaskExecutorServices();
        } catch (Throwable t) {
            final TaskManagerException exception =
                    new TaskManagerException(
                            String.format("Could not start the TaskExecutor %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startRegistrationTimeout();
    }

    private void startTaskExecutorServices() throws Exception {
        try {
            // start by connecting to the ResourceManager
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

            // tell the task slot table who's responsible for the task slot actions
            taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

            // start the job leader service
            jobLeaderService.start(
                    getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

            fileCache =
                    new FileCache(
                            taskManagerConfiguration.getTmpDirectories(),
                            blobCacheService.getPermanentBlobService());
        } catch (Exception e) {
            handleStartTaskExecutorServicesException(e);
        }
    }

    private void handleStartTaskExecutorServicesException(Exception e) throws Exception {
        try {
            stopTaskExecutorServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /** Called to shut down the TaskManager. The method closes all TaskManager services. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping TaskExecutor {}.", getAddress());

        Throwable jobManagerDisconnectThrowable = null;

        FlinkException cause = new FlinkException("The TaskExecutor is shutting down.");

        closeResourceManagerConnection(cause);

        for (JobTable.Job job : jobTable.getJobs()) {
            try {
                closeJob(job, cause);
            } catch (Throwable t) {
                jobManagerDisconnectThrowable =
                        ExceptionUtils.firstOrSuppressed(t, jobManagerDisconnectThrowable);
            }
        }

        Preconditions.checkState(jobTable.isEmpty());

        final Throwable throwableBeforeTasksCompletion = jobManagerDisconnectThrowable;

        return FutureUtils.runAfterwards(taskSlotTable.closeAsync(), this::stopTaskExecutorServices)
                .handle(
                        (ignored, throwable) -> {
                            handleOnStopException(throwableBeforeTasksCompletion, throwable);
                            return null;
                        });
    }

    private void handleOnStopException(
            Throwable throwableBeforeTasksCompletion, Throwable throwableAfterTasksCompletion) {
        final Throwable throwable;

        if (throwableBeforeTasksCompletion != null) {
            throwable =
                    ExceptionUtils.firstOrSuppressed(
                            throwableBeforeTasksCompletion, throwableAfterTasksCompletion);
        } else {
            throwable = throwableAfterTasksCompletion;
        }

        if (throwable != null) {
            throw new CompletionException(
                    new FlinkException("Error while shutting the TaskExecutor down.", throwable));
        } else {
            log.info("Stopped TaskExecutor {}.", getAddress());
        }
    }

    private void stopTaskExecutorServices() throws Exception {
        Exception exception = null;

        try {
            threadInfoSampleService.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskExecutorServices.shutDown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            fileCache.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        // it will call close() recursively from the parent to children
        taskManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ======================================================================
    //  RPC methods
    // ======================================================================

    @Override
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            final ExecutionAttemptID taskExecutionAttemptId,
            final ThreadInfoSamplesRequest requestParams,
            final Time timeout) {

        final Task task = taskSlotTable.getTask(taskExecutionAttemptId);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            String.format(
                                    "Cannot sample task %s. "
                                            + "Task is not known to the task manager.",
                                    taskExecutionAttemptId)));
        }

        final CompletableFuture<List<ThreadInfoSample>> stackTracesFuture =
                threadInfoSampleService.requestThreadInfoSamples(
                        SampleableTaskAdapter.fromTask(task), requestParams);

        return stackTracesFuture.thenApply(TaskThreadInfoResponse::new);
    }

    // ----------------------------------------------------------------------
    // Task lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {

        try {
            final JobID jobId = tdd.getJobId();
            final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

            final JobTable.Connection jobManagerConnection =
                    jobTable.getConnection(jobId)
                            .orElseThrow(
                                    () -> {
                                        final String message =
                                                "Could not submit task because there is no JobManager "
                                                        + "associated for the job "
                                                        + jobId
                                                        + '.';

                                        log.debug(message);
                                        return new TaskSubmissionException(message);
                                    });

            if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
                final String message =
                        "Rejecting the task submission because the job manager leader id "
                                + jobMasterId
                                + " does not match the expected job manager leader id "
                                + jobManagerConnection.getJobMasterId()
                                + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
                final String message =
                        "No task slot allocated for job ID "
                                + jobId
                                + " and allocation ID "
                                + tdd.getAllocationId()
                                + '.';
                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            // re-integrate offloaded data:
            try {
                tdd.loadBigData(blobCacheService.getPermanentBlobService());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
            }

            // deserialize the pre-serialized information
            final JobInformation jobInformation;
            final TaskInformation taskInformation;
            try {
                jobInformation =
                        tdd.getSerializedJobInformation()
                                .deserializeValue(getClass().getClassLoader());
                taskInformation =
                        tdd.getSerializedTaskInformation()
                                .deserializeValue(getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not deserialize the job or task information.", e);
            }

            if (!jobId.equals(jobInformation.getJobId())) {
                throw new TaskSubmissionException(
                        "Inconsistent job ID information inside TaskDeploymentDescriptor ("
                                + tdd.getJobId()
                                + " vs. "
                                + jobInformation.getJobId()
                                + ")");
            }

            TaskMetricGroup taskMetricGroup =
                    taskManagerMetricGroup.addTaskForJob(
                            jobInformation.getJobId(),
                            jobInformation.getJobName(),
                            taskInformation.getJobVertexId(),
                            tdd.getExecutionAttemptId(),
                            taskInformation.getTaskName(),
                            tdd.getSubtaskIndex(),
                            tdd.getAttemptNumber());

            InputSplitProvider inputSplitProvider =
                    new RpcInputSplitProvider(
                            jobManagerConnection.getJobManagerGateway(),
                            taskInformation.getJobVertexId(),
                            tdd.getExecutionAttemptId(),
                            taskManagerConfiguration.getRpcTimeout());

            final TaskOperatorEventGateway taskOperatorEventGateway =
                    new RpcTaskOperatorEventGateway(
                            jobManagerConnection.getJobManagerGateway(),
                            executionAttemptID,
                            (t) -> runAsync(() -> failTask(executionAttemptID, t)));

            TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
            CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
            GlobalAggregateManager aggregateManager =
                    jobManagerConnection.getGlobalAggregateManager();

            LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
                    jobManagerConnection.getClassLoaderHandle();
            ResultPartitionConsumableNotifier resultPartitionConsumableNotifier =
                    jobManagerConnection.getResultPartitionConsumableNotifier();
            PartitionProducerStateChecker partitionStateChecker =
                    jobManagerConnection.getPartitionStateChecker();

            final TaskLocalStateStore localStateStore =
                    localStateStoresManager.localStateStoreForSubtask(
                            jobId,
                            tdd.getAllocationId(),
                            taskInformation.getJobVertexId(),
                            tdd.getSubtaskIndex());

            final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

            final TaskStateManager taskStateManager =
                    new TaskStateManagerImpl(
                            jobId,
                            tdd.getExecutionAttemptId(),
                            localStateStore,
                            taskRestore,
                            checkpointResponder);

            MemoryManager memoryManager;
            try {
                memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
            } catch (SlotNotFoundException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            Task task =
                    new Task(
                            jobInformation,
                            taskInformation,
                            tdd.getExecutionAttemptId(),
                            tdd.getAllocationId(),
                            tdd.getSubtaskIndex(),
                            tdd.getAttemptNumber(),
                            tdd.getProducedPartitions(),
                            tdd.getInputGates(),
                            memoryManager,
                            taskExecutorServices.getIOManager(),
                            taskExecutorServices.getShuffleEnvironment(),
                            taskExecutorServices.getKvStateService(),
                            taskExecutorServices.getBroadcastVariableManager(),
                            taskExecutorServices.getTaskEventDispatcher(),
                            externalResourceInfoProvider,
                            taskStateManager,
                            taskManagerActions,
                            inputSplitProvider,
                            checkpointResponder,
                            taskOperatorEventGateway,
                            aggregateManager,
                            classLoaderHandle,
                            fileCache,
                            taskManagerConfiguration,
                            taskMetricGroup,
                            resultPartitionConsumableNotifier,
                            partitionStateChecker,
                            getRpcService().getExecutor());

            taskMetricGroup.gauge(MetricNames.IS_BACK_PRESSURED, task::isBackPressured);

            log.info(
                    "Received task {} ({}), deploy into slot with allocation id {}.",
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    tdd.getExecutionAttemptId(),
                    tdd.getAllocationId());

            boolean taskAdded;

            try {
                taskAdded = taskSlotTable.addTask(task);
            } catch (SlotNotFoundException | SlotNotActiveException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            if (taskAdded) {
                task.startTaskThread();

                setupResultPartitionBookkeeping(
                        tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                final String message =
                        "TaskManager already contains a task for id " + task.getExecutionId() + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }
        } catch (TaskSubmissionException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private void setupResultPartitionBookkeeping(
            JobID jobId,
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions,
            CompletableFuture<ExecutionState> terminationFuture) {
        final Set<ResultPartitionID> partitionsRequiringRelease =
                filterPartitionsRequiringRelease(producedResultPartitions)
                        .peek(
                                rpdd ->
                                        partitionTracker.startTrackingPartition(
                                                jobId, TaskExecutorPartitionInfo.from(rpdd)))
                        .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                        .map(ShuffleDescriptor::getResultPartitionID)
                        .collect(Collectors.toSet());

        final CompletableFuture<ExecutionState> taskTerminationWithResourceCleanupFuture =
                terminationFuture.thenApplyAsync(
                        executionState -> {
                            if (executionState != ExecutionState.FINISHED) {
                                partitionTracker.stopTrackingPartitions(partitionsRequiringRelease);
                            }
                            return executionState;
                        },
                        getMainThreadExecutor());

        taskResultPartitionCleanupFuturesPerJob.compute(
                jobId,
                (ignored, completableFutures) -> {
                    if (completableFutures == null) {
                        completableFutures = new ArrayList<>(4);
                    }

                    completableFutures.add(taskTerminationWithResourceCleanupFuture);
                    return completableFutures;
                });
    }

    private Stream<ResultPartitionDeploymentDescriptor> filterPartitionsRequiringRelease(
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions) {
        return producedResultPartitions.stream()
                // only blocking partitions require explicit release call
                .filter(d -> d.getPartitionType().isBlocking())
                // partitions without local resources don't store anything on the TaskExecutor
                .filter(d -> d.getShuffleDescriptor().storesLocalResourcesOn().isPresent());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                task.cancelExecution();
                return CompletableFuture.completedFuture(Acknowledge.get());
            } catch (Throwable t) {
                return FutureUtils.completedExceptionally(
                        new TaskException(
                                "Cannot cancel task for execution " + executionAttemptID + '.', t));
            }
        } else {
            final String message =
                    "Cannot find task to stop for execution " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskException(message));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> pauseTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout) {
        System.out.println("TaskExecutor receives pause! current thread = "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            task.pause();
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<?> sendControlToTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout, ControlMessage controlMessage) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            if (controlMessage instanceof ReconfigurationControlMessage
                    || controlMessage instanceof StartMonitoringDataDistrControlMessage) {
                task.sendControl(controlMessage);
            } else if (controlMessage instanceof StopMonitoringDataDistrControlMessage) {
                StopMonitoringDataDistrControlMessage stopControlMessage =
                        (StopMonitoringDataDistrControlMessage) controlMessage;
                if (stopControlMessage.getType() == StopMonitoringDataDistrControlMessage.Type.FILTER) {
                    FilterDataDistrMergeStats filterDataDistrStats = task.sendStopDataDistrControlFilter((StopMonitoringDataDistrControlMessage)controlMessage);
                    return CompletableFuture.completedFuture(filterDataDistrStats);
                } else if (stopControlMessage.getType() == StopMonitoringDataDistrControlMessage.Type.JOIN) {
                    JoinDataDistrMergeStats joinDataDistrStats = task.sendStopDataDistrControlJoin((StopMonitoringDataDistrControlMessage)controlMessage);
                    return CompletableFuture.completedFuture(joinDataDistrStats);
                }

            } else {
                throw new RuntimeException("Unsupported control message type");
            }
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    public CompletableFuture<TaskUtilizationStats> sendMonitoringMsgToTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            TaskUtilizationStats metrics = task.sendMonitoringMsg();
            return CompletableFuture.completedFuture(metrics);
        }
        throw new RuntimeException("Attempting to send monitoring message to a task that is not in"
                + "the task slot table.");
    }

    public CompletableFuture<Double> sendThroughputMonitoringMsgToTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout
    ) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            double throughput = task.sendThroughputMonitoringMsg();
            return CompletableFuture.completedFuture(throughput);
        }
        throw new RuntimeException("Attempting to send monitoring message to a task that is not in"
                + "the task slot table.");
    }

    public CompletableFuture<DataDistrSplitStats> sendSplitPhaseMonitoringMsgToTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout
    ) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            DataDistrSplitStats splitStats = task.sendSplitPhaseMonitoringMsg();
            return CompletableFuture.completedFuture(splitStats);
        }
        throw new RuntimeException("Attempting to send monitoring message to a task that is not in"
                + "the task slot table.");
    }

    private synchronized boolean tryToSendState(int activeGroupID){
        ReconfData reconfData = reconfDataPerActiveGroup.get(activeGroupID);
        log.debug(GROUP_SHARE, "State migration: Task Manager is trying to send the state. "
                + "Active group ID: " + activeGroupID +
                " sendersTotal: " + reconfData.totalNumOfSenders +
                " sendersSoFar: " + reconfData.senders.size() +
                " receiversTotal: " + reconfData.totalNumOfReceivers +
                " receiversSoFar: " + reconfData.receivers.size());
        if (reconfData.totalNumOfSenders > reconfData.senders.size() ||
                reconfData.totalNumOfReceivers > reconfData.receivers.size()){
            // not all servers and receivers have notified the task manager yet
            return false;
        }

        for (Map.Entry<Integer, ExecutionAttemptID> receiver : reconfData.receivers.entrySet()){
            final Task receiverTask = taskSlotTable.getTask(receiver.getValue());
            if (receiverTask == null){
                throw new RuntimeException("Receiver task not found in the task slot table");
                // maybe this can happen if I terminate a job while the state migration is ongoing and
                // then start a new job
            }
            receiverTask.sendStateToReceiver(reconfData.stateToBeMigrated.get(receiver.getKey()),
                    reconfData.stateToBeMigratedPassiveQuery.get(receiver.getKey()),
                    reconfData.dedupMapsToBeMigrated.get(receiver.getKey()),
                    reconfData.triggersToBeMigrated.get(receiver.getKey()));
            log.debug(GROUP_SHARE, "State migration: Task Manager sent the state to the receiver"
                    + " active-id " + activeGroupID);
        }

        if (reconfData.multipleTMsInvolvedInReconfiguration) {
            if (jobTable.getJobs().size() == 1) {
                //log.debug(GROUP_SHARE, "Sending state to job master");

                JobMasterGateway jobMasterGateway = getJobMasterGateway();

                if (log.isDebugEnabled(GROUP_SHARE)) {
                    //compute the memory size of stateForOtherTMs
                    long sizeOfStateForOtherTMs = sizeOfState(reconfData.stateForOtherTMs);
                    long sizeOfStateForOtherTMsPassiveQuery = sizeOfState(reconfData.stateForOtherTMsPassiveQuery);
                    long sizeOfDedupMapsForOtherTMs = sizeOfDedupMaps(reconfData.deduplicationMapsForOtherTMs);
                    long sizeOfTriggers = sizeOfTriggers(reconfData.triggersForOtherTMs);
                    long sizeOfStateNameDict = 68
                            * reconfData.stateNameDict.size(); // 64 would be the approx size for a 10 character string + 4 for the key
                    log.debug(GROUP_SHARE, "Size of state for JobMaster: " + sizeOfStateForOtherTMs + " " +
                            sizeOfStateForOtherTMsPassiveQuery + " " + sizeOfDedupMapsForOtherTMs
                            + " " +
                            sizeOfTriggers + " " + sizeOfStateNameDict + " total " +
                            (sizeOfStateForOtherTMs + sizeOfStateForOtherTMsPassiveQuery
                                    + sizeOfDedupMapsForOtherTMs + sizeOfTriggers
                                    + sizeOfStateNameDict + 4));
                }

                Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>>[] stateDividedInFour = new Map[4];
                for (int i = 0; i < 4; i++) {
                    stateDividedInFour[i] = new HashMap<>();
                }

                for (Map.Entry<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> entry :
                        reconfData.stateForOtherTMs.entrySet()) {
                    for (int i = 0; i < 4; i++) {
                        stateDividedInFour[i].put(entry.getKey(), new HashMap<>());
                    }
                    int i = 0;
                    for (Map.Entry<Integer, Map<Integer, Map<Integer, byte[]>>> entry2 : entry
                            .getValue()
                            .entrySet()) {
                        int index = i % 4;
                        stateDividedInFour[index].get(entry.getKey()).put(entry2.getKey(), entry2.getValue());
                        i++;
                    }
                }

                //jobMasterGateway.sendStateOnlyToJobMaster(stateForOtherTMs, numOfSenderTMs);
                for (int i = 0; i < 4; i++) {
                    jobMasterGateway.sendStateOnlyToJobMaster(stateDividedInFour[i],
                            reconfData.stateNameDict, reconfData.numOfSenderTMs, activeGroupID);
                }
                jobMasterGateway.sendDedupMapsToJobMaster(
                        reconfData.deduplicationMapsForOtherTMs, reconfData.numOfSenderTMs, activeGroupID);
                jobMasterGateway.sendTriggersToJobMaster(
                        reconfData.triggersForOtherTMs, reconfData.numOfSenderTMs, activeGroupID);
                jobMasterGateway.sendStatePassiveToJobMaster(
                        reconfData.stateForOtherTMsPassiveQuery,
                        reconfData.stateNameDict, reconfData.numOfSenderTMs, activeGroupID);

                log.debug(GROUP_SHARE, "State migration: Task Manager sent the serialized state to the job master"
                        + " active-id " + activeGroupID);
            } else {
                throw new RuntimeException(
                        "More than one job found in the job table. We do not support"
                                + " multiple jobs.");
            }
        }
        if (!reconfData.multipleTMsInvolvedInReconfiguration || reconfData.receivers.isEmpty()) {
            clearStateMigrationDataStructures(activeGroupID);
        }
        if (pendingSerializedStatePerActiveGroup.containsKey(activeGroupID)) {
            PendingSerializedState pendingState = pendingSerializedStatePerActiveGroup.get(activeGroupID);
            sendSerializedStateToReceivers(reconfData, pendingState.state, pendingState.dedupMaps,
                    pendingState.triggers, pendingState.statePassiveQuery, pendingState.stateNameDict,
                    activeGroupID);
        }

        return true;
    }

    private long sizeOfState(Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> state){
        long size = 0;
        for (Map.Entry<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> entry : state.entrySet()) {
            // size of resource id
            size += entry.getKey().getResourceID().toString().length() * 2 +
                    entry.getKey().getMetadata().length() * 2 + 8;
            for (Map.Entry<Integer, Map<Integer, Map<Integer, byte[]>>> entry2 : entry.getValue().entrySet()) {
                // size of partition id
                size += 4;
                for (Map.Entry<Integer, Map<Integer, byte[]>> entry3 : entry2.getValue().entrySet()) {
                    // size of state name id
                    size += 4;
                    for (Map.Entry<Integer, byte[]> entry4 : entry3.getValue().entrySet()) {
                        // size of key group state
                        size += 4 + entry4.getValue().length;
                    }
                }
            }
        }

        return size;
    }

    private long sizeOfDedupMaps( Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> dedupMaps) {
        long size = 0;
        for (Map.Entry<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>>
                entry : dedupMaps.entrySet()) {
            size += entry.getKey().getResourceID().toString().length() * 2 +
                    entry.getKey().getMetadata().length() * 2 + 8;
            for (Map.Entry<Integer, Map<Integer, HashMap<byte[], byte[]>>> entry2 : entry.getValue().entrySet()) {
                size += 4;
                for (Map.Entry<Integer, HashMap<byte[], byte[]>> entry3 : entry2.getValue().entrySet()) {
                    size += 4;
                    for (Map.Entry<byte[], byte[]> entry4 : entry3.getValue().entrySet()) {
                        size += entry4.getKey().length + entry4.getValue().length;
                    }
                }
            }
        }
        return size;
    }

    private long sizeOfTriggers(Map<ResourceID, Map<Integer, List<byte[]>>> triggers) {
        long size = 0;
        for (Map.Entry<ResourceID, Map<Integer, List<byte[]>>> entry : triggers.entrySet()) {
            size += entry.getKey().getResourceID().toString().length() * 2 +
                    entry.getKey().getMetadata().length() * 2 + 8;
            for (Map.Entry<Integer, List<byte[]>> entry2 : entry.getValue().entrySet()) {
                size += 4;
                for (byte[] trigger : entry2.getValue()) {
                    size += trigger.length;
                }
            }
        }
        return size;
    }

    private JobMasterGateway getJobMasterGateway() {
        JobID jobId = jobTable.getJobs().iterator().next().getJobId();
        JobTable.Connection jobManagerConnection = jobTable.getConnection(jobId)
                .orElseThrow(() -> new RuntimeException("JobManager connection not found"));
        return jobManagerConnection.getJobManagerGateway();
    }

    private void clearStateMigrationDataStructures(int activeGroupId) {
        ReconfData reconfData = reconfDataPerActiveGroup.get(activeGroupId);
        reconfData.senders.clear();
        reconfData.receivers.clear();
        reconfData.totalNumOfSenders = 0;
        reconfData.totalNumOfReceivers = 0;
        reconfData.stateToBeMigrated.clear();
        reconfData.stateToBeMigratedPassiveQuery.clear();
        reconfData.dedupMapsToBeMigrated.clear();
        reconfData.triggersToBeMigrated.clear();
        reconfData.stateForOtherTMs.clear();
        reconfData.stateForOtherTMsPassiveQuery.clear();
        reconfData.stateNameDict.clear();
        reconfData.deduplicationMapsForOtherTMs.clear();
        reconfData.triggersForOtherTMs.clear();
        reconfData.multipleTMsInvolvedInReconfiguration = false;
        reconfData.numOfSenderTMs = 0;
        reconfDataPool.add(reconfDataPerActiveGroup.remove(activeGroupId));
    }

    private synchronized boolean tryToSendStateDownstream(int activeGroupID){
        ReconfDataDownstream reconfData = reconfDataPerActiveGroupDownstream.get(activeGroupID);
        if (reconfData.totalNumOfDownstreamSenders > reconfData.stateOfDownstream.size() ||
                reconfData.totalNumOfDownstreamReceivers > reconfData.downstreamReceivers.size() ||
                reconfData.totalNumOfDownstreamReceivers == 0 || reconfData.totalNumOfDownstreamSenders == 0) {
            // not all servers and receivers have notified the task manager yet
            return false;
        }
        // first send the state to the tasks that are on the same TM
        for (Map.Entry<Integer, ExecutionAttemptID> receiver : reconfData.downstreamReceivers.entrySet()) {
            final Task receiverTask = taskSlotTable.getTask(receiver.getValue());
            HeapPriorityQueueElement[] triggers = null;
            int queueSize = 0;
            if (reconfData.triggersOfDownstream.containsKey(receiver.getKey())) {
                triggers = reconfData.triggersOfDownstream.get(receiver.getKey()).triggers;
                queueSize = reconfData.triggersOfDownstream.get(receiver.getKey()).queueSize;
            }
            receiverTask.sendStateToReceiverDownstream(reconfData.stateOfDownstream.get(receiver.getKey()),
                    reconfData.dedupMapsOfDownstream.get(receiver.getKey()), triggers, queueSize);
            log.debug(GROUP_SHARE, "State migration: Task Manager sent the state to the downstream receiver");
        }
        if (reconfData.multipleTMsInvolvedInReconfigurationDownstream) {
            if (jobTable.getJobs().size() == 1) {
                log.debug(GROUP_SHARE, "Sending downstream state to job master");

                JobMasterGateway jobMasterGateway = getJobMasterGateway();

                // send state
                jobMasterGateway.sendStateToJobMasterDownstream(
                        reconfData.stateDownstreamForOtherTMs, reconfData.dedupMapsOfDownstreamForOtherTMs,
                        reconfData.triggersOfDownstreamForOtherTMs, reconfData.queueSizeOfDownstreamForOtherTMs,
                        reconfData.numOfSenderTMsForDownstream, activeGroupID);

                log.debug(GROUP_SHARE, "State migration: Task Manager sent the serialized downstream state to the jobmaster");
            } else {
                throw new RuntimeException(
                        "More than one job found in the job table. We do not support"
                                + " multiple jobs.");
            }
        }
        else {
            clearStateMigrationDataStructuresDownstream(activeGroupID);
        }

        return true;
    }

    private void clearStateMigrationDataStructuresDownstream(int activeGroupId) {
        ReconfDataDownstream reconfData = reconfDataPerActiveGroupDownstream.get(activeGroupId);
        // clear/initialize data structures
        reconfData.downstreamReceivers.clear();
        reconfData.totalNumOfDownstreamSenders = 0;
        reconfData.totalNumOfDownstreamReceivers = 0;
        reconfData.stateOfDownstream.clear();
        reconfData.dedupMapsOfDownstream.clear();
        reconfData.triggersOfDownstream.clear();
        reconfData.stateDownstreamForOtherTMs.clear();
        reconfData.dedupMapsOfDownstreamForOtherTMs.clear();
        reconfData.triggersOfDownstreamForOtherTMs.clear();
        reconfData.queueSizeOfDownstreamForOtherTMs.clear();
        reconfData.numOfSenderTMsForDownstream = 0;
        reconfData.multipleTMsInvolvedInReconfigurationDownstream = false;
        reconfDataPoolDownstream.add(reconfDataPerActiveGroupDownstream.remove(activeGroupId));
    }

    private void saveReceiver(int receiverId, ExecutionAttemptID executionAttemptID, ReconfData reconfData) {
        assert !reconfData.receivers.containsKey(receiverId);
        reconfData.receivers.put(receiverId, executionAttemptID);
    }

    private void saveSender(int senderId, ReconfData reconfData) {
        assert !reconfData.senders.contains(senderId);
        reconfData.senders.add(senderId);
    }

    private void saveDownstreamReceiver(
            int receiverId, ExecutionAttemptID executionAttemptID, ReconfDataDownstream reconfData) {
        assert !reconfData.downstreamReceivers.containsKey(receiverId);
        reconfData.downstreamReceivers.put(receiverId, executionAttemptID);
    }

    private void setNumOfSourcesActive(int numOfSources, ReconfDataSource reconfData){
        // the following asserts that either the totalNumOfSourcesActive has not been set or it has been
        // set by another source to the same value
        assert reconfData.totalNumOfSourcesActive == 0 || reconfData.totalNumOfSourcesActive == numOfSources;
        reconfData.totalNumOfSourcesActive = numOfSources;
    }

    private void setNumOfSourcesPassive(int numOfSources, ReconfDataSource reconfData){
        // the following asserts that either the totalNumOfSourcesPassive has not been set or it has been
        // set by another source to the same value
        assert reconfData.totalNumOfSourcesPassive == 0 || reconfData.totalNumOfSourcesPassive == numOfSources;
        reconfData.totalNumOfSourcesPassive = numOfSources;
    }

    private void setNumOfTMsSources(int numOfTMs, ReconfDataSource reconfData){
        // we take the max from TMs of active and passive query
        if (reconfData.numOfTMsWithSources < numOfTMs) {
            reconfData.numOfTMsWithSources = numOfTMs;
        }
    }

    private void setResourceId(ResourceID resourceId){
        this.resourceIDOfThisTM = resourceId;
    }

    private void setNumOfReceivers(int numOfReceivers, ReconfData reconfData){
        // the following asserts that either the totalNumOfReceivers has not been set or it has been
        // set by another receiver to the same value
        assert reconfData.totalNumOfReceivers == 0 || reconfData.totalNumOfReceivers == numOfReceivers;
        reconfData.totalNumOfReceivers = numOfReceivers;
    }

    private void setNumOfSenders(int numOfSenders, ReconfData reconfData) {
        // the following asserts that either the totalNumOfSenders has not been set or it has been
        // set by another receiver to the same value
        assert reconfData.totalNumOfSenders == 0 || reconfData.totalNumOfSenders == numOfSenders;
        reconfData.totalNumOfSenders = numOfSenders;
    }

    private void setNumOfDownstreamReceivers(int numOfReceivers, ReconfDataDownstream reconfData){
        // the following asserts that either the totalNumOfDownstreamReceivers has not been set or it has been
        // set by another receiver to the same value
        assert reconfData.totalNumOfDownstreamReceivers == 0 || reconfData.totalNumOfDownstreamReceivers == numOfReceivers;
        reconfData.totalNumOfDownstreamReceivers = numOfReceivers;
    }

    private void setNumOfDownstreamSenders(int numOfSenders, ReconfDataDownstream reconfData) {
        // the following asserts that either the totalNumOfSenders has not been set or it has been
        // set by another receiver to the same value
        assert reconfData.totalNumOfDownstreamSenders == 0 || reconfData.totalNumOfDownstreamSenders == numOfSenders;
        reconfData.totalNumOfDownstreamSenders = numOfSenders;
    }

    private void addState(Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> newState,
                          Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> aggrState) {
        for (Map.Entry<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> perPartitionNewState : newState.entrySet()){
            Map<String, Map<Integer, StateMap<?, ?, ?>>> perPartitionState = aggrState
                    .computeIfAbsent(perPartitionNewState.getKey(), x -> new HashMap<>());
            for (Map.Entry<String, Map<Integer, StateMap<?, ?, ?>>> KVStateMap : perPartitionNewState.getValue().entrySet())
            {
                perPartitionState.computeIfAbsent(KVStateMap.getKey(), x -> new HashMap<>()).putAll(KVStateMap.getValue());
            }
        }
    }

    private void addStateForOtherTMs(
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> newState,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> aggrState,
            Map<Integer, String> stateNames, ReconfData reconfData) {
        for (Map.Entry<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> perResourceNewState :
                newState.entrySet()) {
            Map<Integer, Map<Integer, Map<Integer, byte[]>>> perResourceState = aggrState
                    .computeIfAbsent(perResourceNewState.getKey(), x -> new HashMap<>());
            for (Map.Entry<Integer, Map<Integer, Map<Integer, byte[]>>> perPartitionNewState :
                    perResourceNewState.getValue().entrySet()){
                Map<Integer, Map<Integer, byte[]>> perPartitionState = perResourceState
                        .computeIfAbsent(perPartitionNewState.getKey(), x -> new HashMap<>());
                for (Map.Entry<Integer, Map<Integer, byte[]>> perStateNameNewState :
                        perPartitionNewState.getValue().entrySet()){
                    String stateName = stateNames.get(perStateNameNewState.getKey());
                    int stateId = getStateIdFromString(stateName, reconfData);
                    Map<Integer, byte[]> perStateNameState = perPartitionState
                            .computeIfAbsent(stateId, x -> new HashMap<>());
                    for (Map.Entry<Integer, byte[]> perKeyGroupNewState : perStateNameNewState.getValue().entrySet()) {
                        perStateNameState.put(perKeyGroupNewState.getKey(), perKeyGroupNewState.getValue());
                    }
                }
            }
        }
    }

    private int getStateIdFromString(String stateName, ReconfData reconfData) {
        for (Map.Entry<Integer, String> entry : reconfData.stateNameDict.entrySet()) {
            if (entry.getValue().equals(stateName)) {
                return entry.getKey();
            }
        }
        // if not found
        int maxKey = -1;
        for (Map.Entry<Integer, String> entry : reconfData.stateNameDict.entrySet()) {
            if (entry.getKey() > maxKey) {
                maxKey = entry.getKey();
            }
        }
        reconfData.stateNameDict.put(maxKey + 1, stateName);
        return maxKey + 1;
    }

    private void saveStateToBeMigrated(
            Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> newState, ReconfData reconfData) {
        // aggregate the newly received state into stateToBeMigrated
        addState(newState, reconfData.stateToBeMigrated);
    }

    private void saveStateForOtherTMsToBeMigrated(
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> newState,
            Map<Integer, String> stateNameDict, ReconfData reconfData) {
        if (reconfData.stateNameDict.isEmpty()){
            reconfData.stateNameDict = stateNameDict;
        }
        // aggregate the newly received state into stateToBeMigrated
        addStateForOtherTMs(newState, reconfData.stateForOtherTMs, stateNameDict, reconfData);
    }

    private void saveStateToBeMigratedPassiveQuery(
            Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> newState,
            ReconfData reconfData) {
        addState(newState, reconfData.stateToBeMigratedPassiveQuery);
    }

    private void saveStateForOtherTMsToBeMigratedPassive(
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> newState,
            Map<Integer, String> stateNameDict, ReconfData reconfData) {
        if (reconfData.stateNameDict.isEmpty()){
            reconfData.stateNameDict = stateNameDict;
        }
        addStateForOtherTMs(newState, reconfData.stateForOtherTMsPassiveQuery, stateNameDict, reconfData);
    }

    private void saveStateToBeMigratedDownstream(
            int senderId, Map<String, StateMap<?, ?, ?>[]> stateOfDownstream, ReconfDataDownstream reconfData) {
        // Note that the sender and receiver id in this case are the same
        assert reconfData.stateOfDownstream.get(senderId) == null;
        reconfData.stateOfDownstream.put(senderId, stateOfDownstream);
    }

    private void saveStateToBeMigratedDownstreamForOtherTMs(
            int senderId, Map<String, byte[][]> serializedStateOfDownstream,
            ResourceID receiverTMId, ReconfDataDownstream reconfData) {
        // Note that the sender and receiver id in this case are the same
        assert reconfData.stateDownstreamForOtherTMs.get(senderId) == null;
        reconfData.stateDownstreamForOtherTMs.computeIfAbsent(receiverTMId, x -> new HashMap<>(16))
                .put(senderId, serializedStateOfDownstream);
    }

    private void saveWindowStateToBeMigrated(
            Map<Integer, Map<Integer, HashMap<?,?>>> dedupMaps,
            Map<Integer, List<HeapPriorityQueueElement>> triggers, ReconfData reconfData) {
        for (Map.Entry<Integer, Map<Integer, HashMap<?, ?>>> perPartitionNewState : dedupMaps.entrySet()) {
            Map<Integer, HashMap<?, ?>> perPartitionState = reconfData.dedupMapsToBeMigrated
                    .computeIfAbsent(perPartitionNewState.getKey(), x -> new HashMap<>());
            perPartitionState.putAll(perPartitionNewState.getValue());
        }
        for (Map.Entry<Integer, List<HeapPriorityQueueElement>> perPartitionNewState : triggers.entrySet()){
            List<HeapPriorityQueueElement> perPartitionState = reconfData.triggersToBeMigrated.computeIfAbsent(
                    perPartitionNewState.getKey(), x -> new ArrayList<>());
            perPartitionState.addAll(perPartitionNewState.getValue());
        }
    }

    private void saveWindowStateForOtherTMsToBeMigrated(
            Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> deduplicationMapsForOtherTMs,
            Map<ResourceID, Map<Integer, List<byte[]>>> triggersForOtherTMs, ReconfData reconfData) {
        for (Map.Entry<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>>
                perTMNewState : deduplicationMapsForOtherTMs.entrySet()) {
            Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> perTMState =
                    reconfData.deduplicationMapsForOtherTMs.computeIfAbsent(perTMNewState.getKey(), x -> new HashMap<>());
            for (Map.Entry<Integer, Map<Integer, HashMap<byte[], byte[]>>> perPartitionNewState :
                perTMNewState.getValue().entrySet()) {
                Map<Integer, HashMap<byte[], byte[]>> perPartitionState = perTMState
                        .computeIfAbsent(perPartitionNewState.getKey(), x -> new HashMap<>());
                perPartitionState.putAll(perPartitionNewState.getValue());
            }
        }

        for (Map.Entry<ResourceID, Map<Integer, List<byte[]>>> perTMNewState : triggersForOtherTMs.entrySet()) {
            Map<Integer, List<byte[]>> perTMState = reconfData.triggersForOtherTMs.computeIfAbsent(perTMNewState.getKey(), x -> new HashMap<>());
            for (Map.Entry<Integer, List<byte[]>> perPartitionNewState : perTMNewState.getValue().entrySet()) {
                List<byte[]> perPartitionState = perTMState.computeIfAbsent(perPartitionNewState.getKey(), x -> new ArrayList<>());
                perPartitionState.addAll(perPartitionNewState.getValue());
            }
        }
    }

    private void saveWindowStateToBeMigratedDownstream(
            int senderId, HashMap<?, ?>[] dedupMapsOfDownstream,
            HeapPriorityQueueElement[] triggersOfDownstream, int queueSize,
            ReconfDataDownstream reconfData) {
        // Note that the sender and receiver id in this case are the same
        assert reconfData.dedupMapsOfDownstream.get(senderId) == null;
        reconfData.dedupMapsOfDownstream.put(senderId, dedupMapsOfDownstream);
        assert reconfData.triggersOfDownstream.get(senderId) == null;
        reconfData.triggersOfDownstream.put(senderId, new TriggersOfDownstream(triggersOfDownstream, queueSize));
    }

    private void saveWindowStateToBeMigratedDownstreamForOtherTMs(
            int senderId, HashMap<byte[], byte[]>[] dedupMapsForOtherTMs,
            byte[][] triggersForOtherTMs, int queueSize, ResourceID receiverTMId, ReconfDataDownstream reconfData) {
        // Note that the sender and receiver id in this case are the same
        assert reconfData.dedupMapsOfDownstreamForOtherTMs.get(senderId) == null;
        reconfData.dedupMapsOfDownstreamForOtherTMs.computeIfAbsent(receiverTMId, x -> new HashMap<>(16))
                .put(senderId, dedupMapsForOtherTMs);

        assert reconfData.triggersOfDownstreamForOtherTMs.get(senderId) == null;
        reconfData.triggersOfDownstreamForOtherTMs.computeIfAbsent(receiverTMId, x -> new HashMap<>(16))
                .put(senderId, triggersForOtherTMs);
        reconfData.queueSizeOfDownstreamForOtherTMs.computeIfAbsent(receiverTMId, x -> new HashMap<>(16))
                .put(senderId, queueSize);
    }

    private void setConfigForMultiTMs(
            boolean stateMigrationInvolvingManyTMs, int numOfSenderTMs, ReconfData reconfData) {
        assert reconfData.numOfSenderTMs == 0 || reconfData.numOfSenderTMs == numOfSenderTMs;
        reconfData.numOfSenderTMs = numOfSenderTMs;
        reconfData.multipleTMsInvolvedInReconfiguration = stateMigrationInvolvingManyTMs;
    }

    private void setConfigForMultiTMsDownstream(
            boolean stateMigrationInvolvingManyTMs, int numOfSenderTMs, ReconfDataDownstream reconfData) {
        assert reconfData.numOfSenderTMsForDownstream == 0 || reconfData.numOfSenderTMsForDownstream == numOfSenderTMs;
        reconfData.numOfSenderTMsForDownstream = numOfSenderTMs;
        reconfData.multipleTMsInvolvedInReconfigurationDownstream = stateMigrationInvolvingManyTMs;
    }

    private void saveLastTupleData(
            int senderId, ReconfigurableSourceData lastTupleData, int activeGroupID) {
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupID);
        assert reconfData.lastTupleDataMap.get(senderId) == null;
        reconfData.lastTupleDataMap.put(senderId, lastTupleData);

        if (reconfData.lastTupleDataMap.size() == reconfData.totalNumOfSourcesActive &&
                reconfData.sourceTasksOfPasiveQuery.size() == reconfData.totalNumOfSourcesPassive &&
                reconfData.totalNumOfSourcesPassive > 0) {
            sendLastTupleDataToJobMaster(activeGroupID);
        }
    }

    private void saveSourceTask(int taskId, ExecutionAttemptID executionAttemptID, int activeGroupID) {
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupID);
        reconfData.sourceTasksOfPasiveQuery.add(executionAttemptID);

        if (reconfData.sourceTasksOfPasiveQuery.size() == reconfData.totalNumOfSourcesPassive) {
            if (reconfData.lastTupleDataMap.size() == reconfData.totalNumOfSourcesActive
                    && reconfData.totalNumOfSourcesActive > 0) {
                sendLastTupleDataToJobMaster(activeGroupID);
            } else if (reconfData.hostOfNewDriver != "") {
                modifySourceConnection(activeGroupID);
            }
        }
    }

    private synchronized void sendLastTupleDataToJobMaster(int activeGroupID) {
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupID);
        if (reconfData.numOfTMsWithSources > 1) {
            if (jobTable.getJobs().size() == 1) {
                log.debug(GROUP_SHARE, "Sending last tuple data to job master");

                JobMasterGateway jobMasterGateway = getJobMasterGateway();
                jobMasterGateway.sendLastTupleDataToJobMaster(
                        reconfData.lastTupleDataMap, reconfData.numOfTMsWithSources,
                        resourceIDOfThisTM, activeGroupID);
            } else {
                throw new RuntimeException(
                        "More than one job found in the job table. We do not support"
                                + " multiple jobs.");
            }
        } else {
            sendLastTupleDataToSources(activeGroupID);
        }
    }

    private void sendLastTupleDataToSources(int activeGroupId) {
        log.debug(GROUP_SHARE, "Sending last tuple data to sources");
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupId);
        for (ExecutionAttemptID entry : reconfData.sourceTasksOfPasiveQuery) {
            final Task task = taskSlotTable.getTask(entry);
            task.sendLastTupleData(reconfData.lastTupleDataMap);
        }
        reconfData.lastTupleDataMap.clear();
        reconfData.sourceTasksOfPasiveQuery.clear();
        reconfData.totalNumOfSourcesActive = 0;
        reconfData.totalNumOfSourcesPassive = 0;
        reconfData.numOfTMsWithSources = 0;
        reconfDataPoolSource.add(reconfDataPerActiveGroupSource.remove(activeGroupId));
    }

    private synchronized void requestNewDriver(
            String driverJobID, String hostname, String driverPort,
            Set<ResourceID> taskManagerIDsPassiveQuery, int activeGroupID) {
        log.debug(GROUP_SHARE, "Attempting to request a new driver " + driverJobID);
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupID);
        assert !driverJobID.equals(""):
                "task executor received empty driverJobId for new driver request";
        assert reconfData.hostOfNewDriver.equals(""):
                "hostOfNewDriver is not empty while attempting new driver request";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + hostname + ":" + driverPort + "/jobs?toBeCopied=" + driverJobID
                        + "&numCopies=1"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            log.debug(GROUP_SHARE, "Response to the new driver request: " + response.body());
            // Parse the JSON response
            JSONArray jsonArray = new JSONArray(response.body());
            // Check that the array is not empty
            if (jsonArray.length() > 0) {
                // Get the first object from the array
                JSONObject jsonObject = jsonArray.getJSONObject(0);

                // Extract the jobId, hostname, and port
                String newJobId = jsonObject.getString("jobId");
                String newHostname = jsonObject.getString("hostname");
                int newPort = jsonObject.getInt("port");

                sendNewDriverInfoToJobMaster(newHostname, newPort, taskManagerIDsPassiveQuery, activeGroupID);

            }
        } catch (IOException e) {
            throw new RuntimeException("Error with the copy driver request");
        } catch (InterruptedException e) {
            throw new RuntimeException("Error with the copy driver request");
        }
    }

    private synchronized void sendNewDriverInfoToJobMaster(
            String host, int port, Set<ResourceID> taskManagerIDsPassiveQuery, int activeGroupID) {
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupID);
        if (reconfData.numOfTMsWithSources > 1) {
            if (jobTable.getJobs().size() == 1) {
                log.debug(GROUP_SHARE, "Sending new driver info to job master");

                JobMasterGateway jobMasterGateway = getJobMasterGateway();
                jobMasterGateway.sendNewDriverInfoAndResourceIDToJobMaster(
                        host, port, taskManagerIDsPassiveQuery, activeGroupID);
            } else {
                throw new RuntimeException(
                        "More than one job found in the job table. We do not support"
                                + " multiple jobs.");
            }
        }
        else {
            // set newDriverHostAndPort
            reconfData.hostOfNewDriver = host;
            reconfData.portOfNewDriver = port;
            if (reconfData.sourceTasksOfPasiveQuery.size() == reconfData.totalNumOfSourcesPassive
                    && reconfData.totalNumOfSourcesPassive > 0) {
                modifySourceConnection(activeGroupID);
            }
        }
    }

    private void modifySourceConnection(int activeGroupId) {
        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupId);
        for (ExecutionAttemptID entry : reconfData.sourceTasksOfPasiveQuery) {
            final Task task = taskSlotTable.getTask(entry);
            task.modifySourceConnection(reconfData.hostOfNewDriver, reconfData.portOfNewDriver);
        }
        reconfData.hostOfNewDriver = "";
        reconfData.portOfNewDriver = 0;
        reconfData.sourceTasksOfPasiveQuery.clear();
        reconfData.totalNumOfSourcesPassive = 0;
        reconfDataPoolSource.add(reconfDataPerActiveGroupSource.remove(activeGroupId));
    }

    private void increasePoolSize() {
        for (int i = 0; i < 8; i++) {
            reconfDataPool.add(new ReconfData());
        }
    }

    private void increasePoolSizeDownstream() {
        for (int i = 0; i < 8; i++) {
            reconfDataPoolDownstream.add(new ReconfDataDownstream());
        }
    }

    private void increasePoolSizeSource() {
        for (int i = 0; i < 8; i++) {
            reconfDataPoolSource.add(new ReconfDataSource());
        }
    }

    private ReconfData getOrCreateReconfData(int activeGroupId) {
        ReconfData reconfData;
        if (reconfDataPerActiveGroup.containsKey(activeGroupId)) {
            reconfData = reconfDataPerActiveGroup.get(activeGroupId);
        } else {
            if (reconfDataPool.size() == 0) {
                increasePoolSize();
            }
            reconfData = reconfDataPool.remove(reconfDataPool.size() - 1);
            reconfDataPerActiveGroup.put(activeGroupId, reconfData);
        }
        return reconfData;
    }

    private ReconfDataDownstream getOrCreateReconfDataDownstream(int activeGroupId) {
        ReconfDataDownstream reconfData;
        if (reconfDataPerActiveGroupDownstream.containsKey(activeGroupId)) {
            reconfData = reconfDataPerActiveGroupDownstream.get(activeGroupId);
        } else {
            if (reconfDataPoolDownstream.size() == 0) {
                increasePoolSizeDownstream();
            }
            reconfData = reconfDataPoolDownstream.remove(reconfDataPoolDownstream.size() - 1);
            reconfDataPerActiveGroupDownstream.put(activeGroupId, reconfData);
        }
        return reconfData;
    }

    private ReconfDataSource getOrCreateReconfDataSource(int activeGroupId) {
        ReconfDataSource reconfData;
        if (reconfDataPerActiveGroupSource.containsKey(activeGroupId)) {
            reconfData = reconfDataPerActiveGroupSource.get(activeGroupId);
        } else {
            if (reconfDataPoolSource.size() == 0) {
                increasePoolSizeSource();
            }
            reconfData = reconfDataPoolSource.remove(reconfDataPoolSource.size() - 1);
            reconfDataPerActiveGroupSource.put(activeGroupId, reconfData);
        }
        return reconfData;
    }

    @Override
    public CompletableFuture<Acknowledge> resumeTask(
            ExecutionAttemptID executionAttemptID,
            Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if(task != null){
            task.resume();
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------
    // Partition lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            final ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            for (final PartitionInfo partitionInfo : partitionInfos) {
                // Run asynchronously because it might be blocking
                FutureUtils.assertNoException(
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        if (!shuffleEnvironment.updatePartitionInfo(
                                                executionAttemptID, partitionInfo)) {
                                            log.debug(
                                                    "Discard update for input gate partition {} of result {} in task {}. "
                                                            + "The partition is no longer available.",
                                                    partitionInfo
                                                            .getShuffleDescriptor()
                                                            .getResultPartitionID(),
                                                    partitionInfo.getIntermediateDataSetID(),
                                                    executionAttemptID);
                                        }
                                    } catch (IOException | InterruptedException e) {
                                        log.error(
                                                "Could not update input data location for task {}. Trying to fail task.",
                                                task.getTaskInfo().getTaskName(),
                                                e);
                                        task.failExternally(e);
                                    }
                                },
                                getRpcService().getExecutor()));
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            log.debug(
                    "Discard update for input partitions of task {}. Task is no longer running.",
                    executionAttemptID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public void releaseOrPromotePartitions(
            JobID jobId,
            Set<ResultPartitionID> partitionToRelease,
            Set<ResultPartitionID> partitionsToPromote) {
        try {
            partitionTracker.stopTrackingAndReleaseJobPartitions(partitionToRelease);
            partitionTracker.promoteJobPartitions(partitionsToPromote);

            closeJobManagerConnectionIfNoAllocatedResources(jobId);
        } catch (Throwable t) {
            // TODO: Do we still need this catch branch?
            onFatalError(t);
        }

        // TODO: Maybe it's better to return an Acknowledge here to notify the JM about the
        // success/failure with an Exception
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
        partitionTracker.stopTrackingAndReleaseClusterPartitions(dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------
    // Heartbeat RPC
    // ----------------------------------------------------------------------

    @Override
    public void heartbeatFromJobManager(
            ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
        jobManagerHeartbeatManager.requestHeartbeat(resourceID, allocatedSlotReport);
    }

    @Override
    public void heartbeatFromResourceManager(ResourceID resourceID) {
        resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    // ----------------------------------------------------------------------
    // Checkpointing RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        log.debug(
                "Trigger checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
        if (checkpointType.getPostCheckpointAction() == PostCheckpointAction.TERMINATE
                && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
            throw new IllegalArgumentException(
                    "Only synchronous savepoints are allowed to advance the watermark to MAX.");
        }

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received a checkpoint request for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        log.debug(
                "Confirm checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            task.notifyCheckpointComplete(checkpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received a checkpoint confirmation for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        log.debug(
                "Abort checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            task.notifyCheckpointAborted(checkpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received an aborted checkpoint for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }


    // ----------------------------------------------------------------------
    // Slot allocation RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            final SlotID slotId,
            final JobID jobId,
            final AllocationID allocationId,
            final ResourceProfile resourceProfile,
            final String targetAddress,
            final ResourceManagerId resourceManagerId,
            final Time timeout) {
        // TODO: Filter invalid requests from the resource manager by using the
        // instance/registration Id

        log.info(
                "Receive slot request {} for job {} from resource manager with leader id {}.",
                allocationId,
                jobId,
                resourceManagerId);

        if (!isConnectedToResourceManager(resourceManagerId)) {
            final String message =
                    String.format(
                            "TaskManager is not connected to the resource manager %s.",
                            resourceManagerId);
            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskManagerException(message));
        }

        try {
            allocateSlot(slotId, jobId, allocationId, resourceProfile);
        } catch (SlotAllocationException sae) {
            return FutureUtils.completedExceptionally(sae);
        }

        final JobTable.Job job;

        try {
            job =
                    jobTable.getOrCreateJob(
                            jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));
        } catch (Exception e) {
            // free the allocated slot
            try {
                taskSlotTable.freeSlot(allocationId);
            } catch (SlotNotFoundException slotNotFoundException) {
                // slot no longer existent, this should actually never happen, because we've
                // just allocated the slot. So let's fail hard in this case!
                onFatalError(slotNotFoundException);
            }

            // release local state under the allocation id.
            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

            // sanity check
            if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
                onFatalError(new Exception("Could not free slot " + slotId));
            }

            return FutureUtils.completedExceptionally(
                    new SlotAllocationException("Could not create new job.", e));
        }

        if (job.isConnected()) {
            offerSlotsToJobManager(jobId);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private TaskExecutorJobServices registerNewJobAndCreateServices(
            JobID jobId, String targetAddress) throws Exception {
        jobLeaderService.addJob(jobId, targetAddress);
        final PermanentBlobCache permanentBlobService = blobCacheService.getPermanentBlobService();
        permanentBlobService.registerJob(jobId);

        return TaskExecutorJobServices.create(
                libraryCacheManager.registerClassLoaderLease(jobId),
                () -> permanentBlobService.releaseJob(jobId));
    }

    private void allocateSlot(
            SlotID slotId, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile)
            throws SlotAllocationException {
        if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
            if (taskSlotTable.allocateSlot(
                    slotId.getSlotNumber(),
                    jobId,
                    allocationId,
                    resourceProfile,
                    taskManagerConfiguration.getSlotTimeout())) {
                log.info("Allocated slot for {}.", allocationId);
            } else {
                log.info("Could not allocate slot for {}.", allocationId);
                throw new SlotAllocationException("Could not allocate slot.");
            }
        } else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
            final String message =
                    "The slot " + slotId + " has already been allocated for a different job.";

            log.info(message);

            final AllocationID allocationID =
                    taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
            throw new SlotOccupiedException(
                    message, allocationID, taskSlotTable.getOwningJob(allocationID));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        freeSlotInternal(allocationId, cause);

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void freeInactiveSlots(JobID jobId, Time timeout) {
        log.debug("Freeing inactive slots for job {}.", jobId);

        // need a copy to prevent ConcurrentModificationExceptions
        final ImmutableList<TaskSlot<Task>> inactiveSlots =
                ImmutableList.copyOf(taskSlotTable.getAllocatedSlots(jobId));
        for (TaskSlot<Task> slot : inactiveSlots) {
            freeSlotInternal(
                    slot.getAllocationId(),
                    new FlinkException("Slot was re-claimed by resource manager."));
        }
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType, Time timeout) {
        final String filePath;
        switch (fileType) {
            case LOG:
                filePath = taskManagerConfiguration.getTaskManagerLogPath();
                break;
            case STDOUT:
                filePath = taskManagerConfiguration.getTaskManagerStdoutPath();
                break;
            default:
                filePath = null;
        }
        return requestFileUploadByFilePath(filePath, fileType.toString());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName, Time timeout) {
        final String filePath;
        final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
        if (StringUtils.isNullOrWhitespaceOnly(logDir)
                || StringUtils.isNullOrWhitespaceOnly(fileName)) {
            filePath = null;
        } else {
            filePath = new File(logDir, new File(fileName).getName()).getPath();
        }
        return requestFileUploadByFilePath(filePath, fileName);
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(
            Time timeout) {
        return CompletableFuture.completedFuture(
                SerializableOptional.ofNullable(metricQueryServiceAddress));
    }

    // ----------------------------------------------------------------------
    // Disconnection RPCs
    // ----------------------------------------------------------------------

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        jobTable.getConnection(jobId)
                .ifPresent(
                        jobManagerConnection ->
                                disconnectAndTryReconnectToJobManager(jobManagerConnection, cause));
    }

    private void disconnectAndTryReconnectToJobManager(
            JobTable.Connection jobManagerConnection, Exception cause) {
        disconnectJobManagerConnection(jobManagerConnection, cause);
        jobLeaderService.reconnect(jobManagerConnection.getJobId());
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        if (isRunning()) {
            reconnectToResourceManager(cause);
        }
    }

    // ----------------------------------------------------------------------
    // Other RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID executionAttemptID,
            OperatorID operatorId,
            SerializedValue<OperatorEvent> evt) {

        log.debug("Operator event for {} - {}", executionAttemptID, operatorId);

        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException(
                            "Task " + executionAttemptID + " not running on TaskManager"));
        }

        try {
            task.deliverOperatorEvent(operatorId, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            return FutureUtils.completedExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        final Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();

        final Collection<ThreadDumpInfo.ThreadInfo> threadInfos =
                threadDump.stream()
                        .map(
                                threadInfo ->
                                        ThreadDumpInfo.ThreadInfo.create(
                                                threadInfo.getThreadName(), threadInfo.toString()))
                        .collect(Collectors.toList());

        return CompletableFuture.completedFuture(ThreadDumpInfo.create(threadInfos));
    }

    @Override
    public void sendSerializedState(
            Map<Integer, Map<Integer, Map<Integer, byte[]>>> state,
            Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> dedupMaps,
            Map<Integer, List<byte[]>> triggers,
            Map<Integer, Map<Integer, Map<Integer, byte[]>>> statePassiveQuery,
            Map<Integer, String> stateNamesDict, int activeGroupId) {
        ReconfData reconfData = reconfDataPerActiveGroup.get(activeGroupId);
        log.debug(GROUP_SHARE, "Task Manager received serialized state: " + state
                        + " activeGroupId: " + activeGroupId
                        + "receivers " + reconfData.receivers.size() + " out of "
                        + reconfData.totalNumOfReceivers);

        if (reconfData != null && reconfData.receivers.size() == reconfData.totalNumOfReceivers
                && reconfData.senders.size() == reconfData.totalNumOfSenders) {
            sendSerializedStateToReceivers(reconfData, state, dedupMaps, triggers,
                    statePassiveQuery, stateNamesDict, activeGroupId);
        }
        else {
            PendingSerializedState pendingState = new PendingSerializedState(state, dedupMaps,
                    triggers, statePassiveQuery, stateNamesDict);
            pendingSerializedStatePerActiveGroup.put(activeGroupId, pendingState);
        }
    }

    private void sendSerializedStateToReceivers(
            ReconfData reconfData,
            Map<Integer, Map<Integer, Map<Integer, byte[]>>> state,
            Map<Integer, Map<Integer, HashMap<byte[], byte[]>>> dedupMaps,
            Map<Integer, List<byte[]>> triggers,
            Map<Integer, Map<Integer, Map<Integer, byte[]>>> statePassiveQuery,
            Map<Integer, String> stateNamesDict, int activeGroupId) {
        for (Map.Entry<Integer, ExecutionAttemptID> receiver : reconfData.receivers.entrySet()){
            ExecutionAttemptID receiverExecAttempt = receiver.getValue();
            final Task receiverTask = taskSlotTable.getTask(receiverExecAttempt);

            Map<Integer, Map<Integer, byte[]>> stateForPartition = state != null ?
                    state.get(receiver.getKey()) : null;
            Map<Integer, HashMap<byte[], byte[]>> dedupMapsForPartition = dedupMaps != null ?
                    dedupMaps.get(receiver.getKey()) : null;
            List<byte[]> triggersForPartition = triggers != null ?
                    triggers.get(receiver.getKey()) : null;
            Map<Integer, Map<Integer, byte[]>> statePassiveQueryForPartition =
                    statePassiveQuery != null ? statePassiveQuery.get(receiver.getKey()) : null;

            receiverTask.sendSerializedStateToReceiver(
                    stateForPartition,
                    dedupMapsForPartition,
                    triggersForPartition,
                    statePassiveQueryForPartition,
                    stateNamesDict);
            log.debug(GROUP_SHARE, "Task Manager sent serialized state to receiver: " + receiver.getKey());
        }
        clearStateMigrationDataStructures(activeGroupId);
        if (pendingSerializedStatePerActiveGroup.containsKey(activeGroupId)) {
            pendingSerializedStatePerActiveGroup.remove(activeGroupId);
        }
    }

    @Override
    public void sendSerializedStateDownstream(
            Map<Integer, Map<String, byte[][]>> state,
            Map<Integer, HashMap<byte[], byte[]>[]> deduplicationMaps,
            Map<Integer, byte[][]> triggers,
            Map<Integer, Integer> queueSizes, int activeGroupId) {
        log.debug(GROUP_SHARE, "Task Manager received serialized state for downstream");
        ReconfDataDownstream reconfData = reconfDataPerActiveGroupDownstream.get(activeGroupId);

        for (Map.Entry<Integer, ExecutionAttemptID> receiver : reconfData.downstreamReceivers.entrySet()){
            ExecutionAttemptID receiverExecAttempt = receiver.getValue();
            final Task receiverTask = taskSlotTable.getTask(receiverExecAttempt);

            Map<String, byte[][]> stateForPartition = state != null ?
                    state.get(receiver.getKey()) : null;
            HashMap<byte[], byte[]>[] dedupMapsForPartition = deduplicationMaps != null ?
                    deduplicationMaps.get(receiver.getKey()) : null;
            byte[][] triggersForPartition = triggers != null ?
                    triggers.get(receiver.getKey()) : null;
            int queueSize = queueSizes != null ? queueSizes.get(receiver.getKey()) : 0;

            receiverTask.sendSerializedStateToReceiverDownstream(
                    stateForPartition,
                    dedupMapsForPartition,
                    triggersForPartition,
                    queueSize);
        }
        clearStateMigrationDataStructuresDownstream(activeGroupId);
    }

    @Override
    public void sendLastTupleData(Map<Integer, ReconfigurableSourceData> lastTupleData, int activeGroupId) {
        log.debug(GROUP_SHARE, "Task Manager received last tuple data: " + lastTupleData);

        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupId);

        reconfData.lastTupleDataMap = lastTupleData;
        sendLastTupleDataToSources(activeGroupId);
    }

    @Override
    public void sendNewDriverInfo(String host, int port, int activeGroupId) {
        log.debug(GROUP_SHARE, "Task Manager received new driver info: " + host + ":" + port);

        ReconfDataSource reconfData = reconfDataPerActiveGroupSource.get(activeGroupId);

        reconfData.hostOfNewDriver = host;
        reconfData.portOfNewDriver = port;
        if (reconfData.sourceTasksOfPasiveQuery.size() == reconfData.totalNumOfSourcesPassive
                && reconfData.totalNumOfSourcesPassive > 0) {
            modifySourceConnection(activeGroupId);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal resource manager connection methods
    // ------------------------------------------------------------------------

    private void notifyOfNewResourceManagerLeader(
            String newLeaderAddress, ResourceManagerId newResourceManagerId) {
        resourceManagerAddress =
                createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newLeaderAddress, @Nullable ResourceManagerId newResourceManagerId) {
        if (newLeaderAddress == null) {
            return null;
        } else {
            assert (newResourceManagerId != null);
            return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        startRegistrationTimeout();
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (establishedResourceManagerConnection == null);
        assert (resourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        getAddress(),
                        getResourceID(),
                        unresolvedTaskManagerLocation.getDataPort(),
                        JMXService.getPort().orElse(-1),
                        hardwareDescription,
                        memoryConfiguration,
                        taskManagerConfiguration.getDefaultSlotResourceProfile(),
                        taskManagerConfiguration.getTotalResourceProfile());

        resourceManagerConnection =
                new TaskExecutorToResourceManagerConnection(
                        log,
                        getRpcService(),
                        taskManagerConfiguration.getRetryingRegistrationConfiguration(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        getMainThreadExecutor(),
                        new ResourceManagerRegistrationListener(),
                        taskExecutorRegistration);
        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(
            ResourceManagerGateway resourceManagerGateway,
            ResourceID resourceManagerResourceId,
            InstanceID taskExecutorRegistrationId,
            ClusterInformation clusterInformation) {

        final CompletableFuture<Acknowledge> slotReportResponseFuture =
                resourceManagerGateway.sendSlotReport(
                        getResourceID(),
                        taskExecutorRegistrationId,
                        taskSlotTable.createSlotReport(getResourceID()),
                        taskManagerConfiguration.getRpcTimeout());

        slotReportResponseFuture.whenCompleteAsync(
                (acknowledge, throwable) -> {
                    if (throwable != null) {
                        reconnectToResourceManager(
                                new TaskManagerException(
                                        "Failed to send initial slot report to ResourceManager.",
                                        throwable));
                    }
                },
                getMainThreadExecutor());

        // monitor the resource manager as heartbeat target
        resourceManagerHeartbeatManager.monitorTarget(
                resourceManagerResourceId,
                new HeartbeatTarget<TaskExecutorHeartbeatPayload>() {
                    @Override
                    public void receiveHeartbeat(
                            ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
                        resourceManagerGateway.heartbeatFromTaskManager(
                                resourceID, heartbeatPayload);
                    }

                    @Override
                    public void requestHeartbeat(
                            ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
                        // the TaskManager won't send heartbeat requests to the ResourceManager
                    }
                });

        // set the propagated blob server address
        final InetSocketAddress blobServerAddress =
                new InetSocketAddress(
                        clusterInformation.getBlobServerHostname(),
                        clusterInformation.getBlobServerPort());

        blobCacheService.setBlobServerAddress(blobServerAddress);

        establishedResourceManagerConnection =
                new EstablishedResourceManagerConnection(
                        resourceManagerGateway,
                        resourceManagerResourceId,
                        taskExecutorRegistrationId);

        stopRegistrationTimeout();
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            final ResourceID resourceManagerResourceId =
                    establishedResourceManagerConnection.getResourceManagerResourceId();

            if (log.isDebugEnabled()) {
                log.debug("Close ResourceManager connection {}.", resourceManagerResourceId, cause);
            } else {
                log.info("Close ResourceManager connection {}.", resourceManagerResourceId);
            }
            resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceId);

            ResourceManagerGateway resourceManagerGateway =
                    establishedResourceManagerConnection.getResourceManagerGateway();
            resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);

            establishedResourceManagerConnection = null;

            partitionTracker.stopTrackingAndReleaseAllClusterPartitions();
        }

        if (resourceManagerConnection != null) {
            if (!resourceManagerConnection.isConnected()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress(),
                            cause);
                } else {
                    log.info(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress());
                }
            }

            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void startRegistrationTimeout() {
        final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

        if (maxRegistrationDuration != null) {
            final UUID newRegistrationTimeoutId = UUID.randomUUID();
            currentRegistrationTimeoutId = newRegistrationTimeoutId;
            scheduleRunAsync(
                    () -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
        }
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            final Time maxRegistrationDuration =
                    taskManagerConfiguration.getMaxRegistrationDuration();

            onFatalError(
                    new RegistrationTimeoutException(
                            String.format(
                                    "Could not register at the ResourceManager within the specified maximum "
                                            + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                                    maxRegistrationDuration)));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal job manager connection methods
    // ------------------------------------------------------------------------

    private void offerSlotsToJobManager(final JobID jobId) {
        jobTable.getConnection(jobId).ifPresent(this::internalOfferSlotsToJobManager);
    }

    private void internalOfferSlotsToJobManager(JobTable.Connection jobManagerConnection) {
        final JobID jobId = jobManagerConnection.getJobId();

        if (taskSlotTable.hasAllocatedSlots(jobId)) {
            log.info("Offer reserved slots to the leader of job {}.", jobId);

            final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

            final Iterator<TaskSlot<Task>> reservedSlotsIterator =
                    taskSlotTable.getAllocatedSlots(jobId);
            final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

            final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

            while (reservedSlotsIterator.hasNext()) {
                SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
                reservedSlots.add(offer);
            }

            final UUID slotOfferId = UUID.randomUUID();
            currentSlotOfferPerJob.put(jobId, slotOfferId);

            CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture =
                    jobMasterGateway.offerSlots(
                            getResourceID(),
                            reservedSlots,
                            taskManagerConfiguration.getRpcTimeout());

            acceptedSlotsFuture.whenCompleteAsync(
                    handleAcceptedSlotOffers(
                            jobId, jobMasterGateway, jobMasterId, reservedSlots, slotOfferId),
                    getMainThreadExecutor());
        } else {
            log.debug("There are no unassigned slots for the job {}.", jobId);
        }
    }

    @Nonnull
    private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(
            JobID jobId,
            JobMasterGateway jobMasterGateway,
            JobMasterId jobMasterId,
            Collection<SlotOffer> offeredSlots,
            UUID offerId) {
        return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
            // check if this is the latest offer
            if (!offerId.equals(currentSlotOfferPerJob.get(jobId))) {
                // If this offer is outdated then it can be safely ignored.
                // If the response for a given slot is identical in both offers (accepted/rejected),
                // then this is naturally the case since the end-result is the same.
                // If the responses differ, then there are 2 cases to consider:
                // 1) initially rejected, later accepted
                //   This can happen when the resource requirements of a job increases between
                //   offers.
                //   In this case the first response MUST be ignored, so that
                //   the the slot can be properly activated when the second response arrives.
                // 2) initially accepted, later rejected
                //   This can happen when the resource requirements of a job decrease between
                //   offers.
                //   In this case the first response MAY be ignored, because the job no longer
                //   requires the slot (and already has initiated steps to free it) and we can thus
                //   assume that any in-flight task submissions are no longer relevant for the job
                //   execution.

                log.debug(
                        "Discard slot offer response since there is a newer offer for the job {}.",
                        jobId);
                return;
            }

            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    log.info(
                            "Slot offering to JobManager did not finish in time. Retrying the slot offering.");
                    // We ran into a timeout. Try again.
                    offerSlotsToJobManager(jobId);
                } else {
                    log.warn(
                            "Slot offering to JobManager failed. Freeing the slots "
                                    + "and returning them to the ResourceManager.",
                            throwable);

                    // We encountered an exception. Free the slots and return them to the RM.
                    for (SlotOffer reservedSlot : offeredSlots) {
                        freeSlotInternal(reservedSlot.getAllocationId(), throwable);
                    }
                }
            } else {
                // check if the response is still valid
                if (isJobManagerConnectionValid(jobId, jobMasterId)) {
                    // mark accepted slots active
                    for (SlotOffer acceptedSlot : acceptedSlots) {
                        final AllocationID allocationId = acceptedSlot.getAllocationId();
                        try {
                            if (!taskSlotTable.markSlotActive(allocationId)) {
                                // the slot is either free or releasing at the moment
                                final String message =
                                        "Could not mark slot " + allocationId + " active.";
                                log.debug(message);
                                jobMasterGateway.failSlot(
                                        getResourceID(), allocationId, new FlinkException(message));
                            }
                        } catch (SlotNotFoundException e) {
                            final String message =
                                    "Could not mark slot " + allocationId + " active.";
                            jobMasterGateway.failSlot(
                                    getResourceID(), allocationId, new FlinkException(message));
                        }

                        offeredSlots.remove(acceptedSlot);
                    }

                    final Exception e = new Exception("The slot was rejected by the JobManager.");

                    for (SlotOffer rejectedSlot : offeredSlots) {
                        freeSlotInternal(rejectedSlot.getAllocationId(), e);
                    }
                } else {
                    // discard the response since there is a new leader for the job
                    log.debug(
                            "Discard slot offer response since there is a new leader "
                                    + "for the job {}.",
                            jobId);
                }
            }
        };
    }

    private void establishJobManagerConnection(
            JobTable.Job job,
            final JobMasterGateway jobMasterGateway,
            JMTMRegistrationSuccess registrationSuccess) {

        final JobID jobId = job.getJobId();
        final Optional<JobTable.Connection> connection = job.asConnection();

        if (connection.isPresent()) {
            JobTable.Connection oldJobManagerConnection = connection.get();

            if (Objects.equals(
                    oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
                // we already are connected to the given job manager
                log.debug(
                        "Ignore JobManager gained leadership message for {} because we are already connected to it.",
                        jobMasterGateway.getFencingToken());
                return;
            } else {
                disconnectJobManagerConnection(
                        oldJobManagerConnection,
                        new Exception("Found new job leader for job id " + jobId + '.'));
            }
        }

        log.info("Establish JobManager connection for job {}.", jobId);

        ResourceID jobManagerResourceID = registrationSuccess.getResourceID();

        final JobTable.Connection establishedConnection =
                associateWithJobManager(job, jobManagerResourceID, jobMasterGateway);

        // monitor the job manager as heartbeat target
        jobManagerHeartbeatManager.monitorTarget(
                jobManagerResourceID,
                new HeartbeatTarget<TaskExecutorToJobManagerHeartbeatPayload>() {
                    @Override
                    public void receiveHeartbeat(
                            ResourceID resourceID,
                            TaskExecutorToJobManagerHeartbeatPayload payload) {
                        jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
                    }

                    @Override
                    public void requestHeartbeat(
                            ResourceID resourceID,
                            TaskExecutorToJobManagerHeartbeatPayload payload) {
                        // request heartbeat will never be called on the task manager side
                    }
                });

        internalOfferSlotsToJobManager(establishedConnection);
    }

    private void closeJob(JobTable.Job job, Exception cause) {
        job.asConnection()
                .ifPresent(
                        jobManagerConnection ->
                                disconnectJobManagerConnection(jobManagerConnection, cause));

        job.close();
    }

    private void disconnectJobManagerConnection(
            JobTable.Connection jobManagerConnection, Exception cause) {
        final JobID jobId = jobManagerConnection.getJobId();
        if (log.isDebugEnabled()) {
            log.debug("Close JobManager connection for job {}.", jobId, cause);
        } else {
            log.info("Close JobManager connection for job {}.", jobId);
        }

        // 1. fail tasks running under this JobID
        Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

        final FlinkException failureCause =
                new FlinkException(
                        String.format("Disconnect from JobManager responsible for %s.", jobId),
                        cause);

        while (tasks.hasNext()) {
            tasks.next().failExternally(failureCause);
        }

        // 2. Move the active slots to state allocated (possible to time out again)
        Set<AllocationID> activeSlotAllocationIDs =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId);

        final FlinkException freeingCause =
                new FlinkException("Slot could not be marked inactive.");

        for (AllocationID activeSlotAllocationID : activeSlotAllocationIDs) {
            try {
                if (!taskSlotTable.markSlotInactive(
                        activeSlotAllocationID, taskManagerConfiguration.getSlotTimeout())) {
                    freeSlotInternal(activeSlotAllocationID, freeingCause);
                }
            } catch (SlotNotFoundException e) {
                log.debug("Could not mark the slot {} inactive.", activeSlotAllocationID, e);
            }
        }

        // 3. Disassociate from the JobManager
        try {
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerConnection.getResourceId());
            disassociateFromJobManager(jobManagerConnection, cause);
        } catch (IOException e) {
            log.warn(
                    "Could not properly disassociate from JobManager {}.",
                    jobManagerConnection.getJobManagerGateway().getAddress(),
                    e);
        }

        jobManagerConnection.disconnect();
    }

    private JobTable.Connection associateWithJobManager(
            JobTable.Job job, ResourceID resourceID, JobMasterGateway jobMasterGateway) {
        checkNotNull(resourceID);
        checkNotNull(jobMasterGateway);

        TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

        CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);
        GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

        ResultPartitionConsumableNotifier resultPartitionConsumableNotifier =
                new RpcResultPartitionConsumableNotifier(
                        jobMasterGateway,
                        getRpcService().getExecutor(),
                        taskManagerConfiguration.getRpcTimeout());

        PartitionProducerStateChecker partitionStateChecker =
                new RpcPartitionStateChecker(jobMasterGateway);

        registerQueryableState(job.getJobId(), jobMasterGateway);

        return job.connect(
                resourceID,
                jobMasterGateway,
                taskManagerActions,
                checkpointResponder,
                aggregateManager,
                resultPartitionConsumableNotifier,
                partitionStateChecker);
    }

    private void disassociateFromJobManager(
            JobTable.Connection jobManagerConnection, Exception cause) throws IOException {
        checkNotNull(jobManagerConnection);

        final JobID jobId = jobManagerConnection.getJobId();

        // cleanup remaining partitions once all tasks for this job have completed
        scheduleResultPartitionCleanup(jobId);

        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterListener(jobId);
        }

        final KvStateClientProxy kvStateClientProxy = kvStateService.getKvStateClientProxy();

        if (kvStateClientProxy != null) {
            kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobId(), null);
        }

        JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
        jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
    }

    private void handleRejectedJobManagerConnection(
            JobID jobId, String targetAddress, JMTMRegistrationRejection rejection) {
        log.info(
                "The JobManager under {} rejected the registration for job {}: {}. Releasing all job related resources.",
                targetAddress,
                jobId,
                rejection.getReason());

        releaseJobResources(
                jobId,
                new FlinkException(
                        String.format("JobManager %s has rejected the registration.", jobId)));
    }

    private void releaseJobResources(JobID jobId, Exception cause) {
        log.debug("Releasing job resources for job {}.", jobId, cause);

        if (partitionTracker.isTrackingPartitionsFor(jobId)) {
            // stop tracking job partitions
            partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
        }

        // free slots
        final Set<AllocationID> allocationIds = taskSlotTable.getAllocationIdsPerJob(jobId);

        if (!allocationIds.isEmpty()) {
            for (AllocationID allocationId : allocationIds) {
                freeSlotInternal(allocationId, cause);
            }
        }

        jobLeaderService.removeJob(jobId);
        jobTable.getJob(jobId)
                .ifPresent(
                        job -> {
                            closeJob(job, cause);
                        });
        currentSlotOfferPerJob.remove(jobId);
    }

    private void scheduleResultPartitionCleanup(JobID jobId) {
        final Collection<CompletableFuture<ExecutionState>> taskTerminationFutures =
                taskResultPartitionCleanupFuturesPerJob.remove(jobId);
        if (taskTerminationFutures != null) {
            FutureUtils.waitForAll(taskTerminationFutures)
                    .thenRunAsync(
                            () -> {
                                partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
                            },
                            getMainThreadExecutor());
        }
    }

    private void registerQueryableState(JobID jobId, JobMasterGateway jobMasterGateway) {
        final KvStateServer kvStateServer = kvStateService.getKvStateServer();
        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateServer != null && kvStateRegistry != null) {
            kvStateRegistry.registerListener(
                    jobId,
                    new RpcKvStateRegistryListener(
                            jobMasterGateway, kvStateServer.getServerAddress()));
        }

        final KvStateClientProxy kvStateProxy = kvStateService.getKvStateClientProxy();

        if (kvStateProxy != null) {
            kvStateProxy.updateKvStateLocationOracle(jobId, jobMasterGateway);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal task methods
    // ------------------------------------------------------------------------

    private void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                task.failExternally(cause);
            } catch (Throwable t) {
                log.error("Could not fail task {}.", executionAttemptID, t);
            }
        } else {
            log.info(
                    "Cannot find task to fail for execution {} with exception:",
                    executionAttemptID,
                    cause);
        }
    }

    private void updateTaskExecutionState(
            final JobMasterGateway jobMasterGateway, final TaskExecutionState taskExecutionState) {
        final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

        CompletableFuture<Acknowledge> futureAcknowledge =
                jobMasterGateway.updateTaskExecutionState(taskExecutionState);

        futureAcknowledge.whenCompleteAsync(
                (ack, throwable) -> {
                    if (throwable != null) {
                        failTask(executionAttemptID, throwable);
                    }
                },
                getMainThreadExecutor());
    }

    private void unregisterTaskAndNotifyFinalState(
            final JobMasterGateway jobMasterGateway, final ExecutionAttemptID executionAttemptID) {

        Task task = taskSlotTable.removeTask(executionAttemptID);
        if (task != null) {
            if (!task.getExecutionState().isTerminal()) {
                try {
                    task.failExternally(
                            new IllegalStateException("Task is being remove from TaskManager."));
                } catch (Exception e) {
                    log.error("Could not properly fail task.", e);
                }
            }

            log.info(
                    "Un-registering task and sending final execution state {} to JobManager for task {} {}.",
                    task.getExecutionState(),
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    task.getExecutionId());

            AccumulatorSnapshot accumulatorSnapshot = task.getAccumulatorRegistry().getSnapshot();

            updateTaskExecutionState(
                    jobMasterGateway,
                    new TaskExecutionState(
                            task.getExecutionId(),
                            task.getExecutionState(),
                            task.getFailureCause(),
                            accumulatorSnapshot,
                            task.getMetricGroup().getIOMetricGroup().createSnapshot()));
        } else {
            log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
        }
    }

    private void freeSlotInternal(AllocationID allocationId, Throwable cause) {
        checkNotNull(allocationId);

        log.debug("Free slot with allocation id {} because: {}", allocationId, cause.getMessage());

        try {
            final JobID jobId = taskSlotTable.getOwningJob(allocationId);

            final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

            if (slotIndex != -1) {

                if (isConnectedToResourceManager()) {
                    // the slot was freed. Tell the RM about it
                    ResourceManagerGateway resourceManagerGateway =
                            establishedResourceManagerConnection.getResourceManagerGateway();

                    resourceManagerGateway.notifySlotAvailable(
                            establishedResourceManagerConnection.getTaskExecutorRegistrationId(),
                            new SlotID(getResourceID(), slotIndex),
                            allocationId);
                }

                if (jobId != null) {
                    closeJobManagerConnectionIfNoAllocatedResources(jobId);
                }
            }
        } catch (SlotNotFoundException e) {
            log.debug("Could not free slot for allocation id {}.", allocationId, e);
        }

        localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
    }

    private void closeJobManagerConnectionIfNoAllocatedResources(JobID jobId) {
        // check whether we still have allocated slots for the same job
        if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty()
                && !partitionTracker.isTrackingPartitionsFor(jobId)) {
            // we can remove the job from the job leader service

            final FlinkException cause =
                    new FlinkException(
                            "TaskExecutor "
                                    + getAddress()
                                    + " has no more allocated slots for job "
                                    + jobId
                                    + '.');

            releaseJobResources(jobId, cause);
        }
    }

    private void timeoutSlot(AllocationID allocationId, UUID ticket) {
        checkNotNull(allocationId);
        checkNotNull(ticket);

        if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
            freeSlotInternal(
                    allocationId, new Exception("The slot " + allocationId + " has timed out."));
        } else {
            log.debug(
                    "Received an invalid timeout for allocation id {} with ticket {}.",
                    allocationId,
                    ticket);
        }
    }

    /**
     * Syncs the TaskExecutor's view on its allocated slots with the JobMaster's view. Slots which
     * are no longer reported by the JobMaster are being freed. Slots which the JobMaster thinks it
     * still owns but which are no longer allocated to it will be failed via {@link
     * JobMasterGateway#failSlot}.
     *
     * @param jobMasterGateway jobMasterGateway to talk to the connected job master
     * @param allocatedSlotReport represents the JobMaster's view on the current slot allocation
     *     state
     */
    private void syncSlotsWithSnapshotFromJobMaster(
            JobMasterGateway jobMasterGateway, AllocatedSlotReport allocatedSlotReport) {
        failNoLongerAllocatedSlots(allocatedSlotReport, jobMasterGateway);
        freeNoLongerUsedSlots(allocatedSlotReport);
    }

    private void failNoLongerAllocatedSlots(
            AllocatedSlotReport allocatedSlotReport, JobMasterGateway jobMasterGateway) {
        for (AllocatedSlotInfo allocatedSlotInfo : allocatedSlotReport.getAllocatedSlotInfos()) {
            final AllocationID allocationId = allocatedSlotInfo.getAllocationId();
            if (!taskSlotTable.isAllocated(
                    allocatedSlotInfo.getSlotIndex(),
                    allocatedSlotReport.getJobId(),
                    allocationId)) {
                jobMasterGateway.failSlot(
                        getResourceID(),
                        allocationId,
                        new FlinkException(
                                String.format(
                                        "Slot %s on TaskExecutor %s is not allocated by job %s.",
                                        allocatedSlotInfo.getSlotIndex(),
                                        getResourceID().getStringWithMetadata(),
                                        allocatedSlotReport.getJobId())));
            }
        }
    }

    private void freeNoLongerUsedSlots(AllocatedSlotReport allocatedSlotReport) {
        final Set<AllocationID> activeSlots =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(allocatedSlotReport.getJobId());
        final Set<AllocationID> reportedSlots =
                allocatedSlotReport.getAllocatedSlotInfos().stream()
                        .map(AllocatedSlotInfo::getAllocationId)
                        .collect(Collectors.toSet());

        final Sets.SetView<AllocationID> difference = Sets.difference(activeSlots, reportedSlots);

        for (AllocationID allocationID : difference) {
            freeSlotInternal(
                    allocationID,
                    new FlinkException(
                            String.format(
                                    "%s is no longer allocated by job %s.",
                                    allocationID, allocatedSlotReport.getJobId())));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal utility methods
    // ------------------------------------------------------------------------

    private boolean isConnectedToResourceManager() {
        return establishedResourceManagerConnection != null;
    }

    private boolean isConnectedToResourceManager(ResourceManagerId resourceManagerId) {
        return establishedResourceManagerConnection != null
                && resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    private boolean isJobManagerConnectionValid(JobID jobId, JobMasterId jobMasterId) {
        return jobTable.getConnection(jobId)
                .map(jmConnection -> Objects.equals(jmConnection.getJobMasterId(), jobMasterId))
                .orElse(false);
    }

    private CompletableFuture<TransientBlobKey> requestFileUploadByFilePath(
            String filePath, String fileTag) {
        log.debug("Received file upload request for file {}", fileTag);
        if (!StringUtils.isNullOrWhitespaceOnly(filePath)) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        final File file = new File(filePath);
                        if (file.exists()) {
                            try {
                                return putTransientBlobStream(new FileInputStream(file), fileTag)
                                        .get();
                            } catch (Exception e) {
                                log.debug("Could not upload file {}.", fileTag, e);
                                throw new CompletionException(
                                        new FlinkException(
                                                "Could not upload file " + fileTag + '.', e));
                            }
                        } else {
                            log.debug(
                                    "The file {} does not exist on the TaskExecutor {}.",
                                    fileTag,
                                    getResourceID().getStringWithMetadata());
                            throw new CompletionException(
                                    new FlinkException(
                                            "The file "
                                                    + fileTag
                                                    + " does not exist on the TaskExecutor."));
                        }
                    },
                    ioExecutor);
        } else {
            log.debug(
                    "The file {} is unavailable on the TaskExecutor {}.",
                    fileTag,
                    getResourceID().getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new FlinkException(
                            "The file " + fileTag + " is not available on the TaskExecutor."));
        }
    }

    private CompletableFuture<TransientBlobKey> putTransientBlobStream(
            InputStream inputStream, String fileTag) {
        final TransientBlobCache transientBlobService = blobCacheService.getTransientBlobService();
        final TransientBlobKey transientBlobKey;

        try {
            transientBlobKey = transientBlobService.putTransient(inputStream);
        } catch (IOException e) {
            log.debug("Could not upload file {}.", fileTag, e);
            return FutureUtils.completedExceptionally(
                    new FlinkException("Could not upload file " + fileTag + '.', e));
        }
        return CompletableFuture.completedFuture(transientBlobKey);
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public ResourceID getResourceID() {
        return unresolvedTaskManagerLocation.getResourceID();
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    void onFatalError(final Throwable t) {
        try {
            log.error("Fatal error occurred in TaskExecutor {}.", getAddress(), t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  Access to fields for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
        return resourceManagerConnection;
    }

    @VisibleForTesting
    HeartbeatManager<Void, TaskExecutorHeartbeatPayload> getResourceManagerHeartbeatManager() {
        return resourceManagerHeartbeatManager;
    }

    // ------------------------------------------------------------------------
    //  Utility classes
    // ------------------------------------------------------------------------

    /** The listener for leader changes of the resource manager. */
    private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(Exception exception) {
            onFatalError(exception);
        }
    }

    private final class JobLeaderListenerImpl implements JobLeaderListener {

        @Override
        public void jobManagerGainedLeadership(
                final JobID jobId,
                final JobMasterGateway jobManagerGateway,
                final JMTMRegistrationSuccess registrationMessage) {
            runAsync(
                    () ->
                            jobTable.getJob(jobId)
                                    .ifPresent(
                                            job ->
                                                    establishJobManagerConnection(
                                                            job,
                                                            jobManagerGateway,
                                                            registrationMessage)));
        }

        @Override
        public void jobManagerLostLeadership(final JobID jobId, final JobMasterId jobMasterId) {
            log.info(
                    "JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);

            runAsync(
                    () ->
                            jobTable.getConnection(jobId)
                                    .ifPresent(
                                            jobManagerConnection ->
                                                    disconnectJobManagerConnection(
                                                            jobManagerConnection,
                                                            new Exception(
                                                                    "Job leader for job id "
                                                                            + jobId
                                                                            + " lost leadership."))));
        }

        @Override
        public void handleError(Throwable throwable) {
            onFatalError(throwable);
        }

        @Override
        public void jobManagerRejectedRegistration(
                JobID jobId, String targetAddress, JMTMRegistrationRejection rejection) {
            runAsync(() -> handleRejectedJobManagerConnection(jobId, targetAddress, rejection));
        }
    }

    private final class ResourceManagerRegistrationListener
            implements RegistrationConnectionListener<
                    TaskExecutorToResourceManagerConnection,
                    TaskExecutorRegistrationSuccess,
                    TaskExecutorRegistrationRejection> {

        @Override
        public void onRegistrationSuccess(
                TaskExecutorToResourceManagerConnection connection,
                TaskExecutorRegistrationSuccess success) {
            final ResourceID resourceManagerId = success.getResourceManagerId();
            final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
            final ClusterInformation clusterInformation = success.getClusterInformation();
            final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (resourceManagerConnection == connection) {
                            try {
                                establishResourceManagerConnection(
                                        resourceManagerGateway,
                                        resourceManagerId,
                                        taskExecutorRegistrationId,
                                        clusterInformation);
                            } catch (Throwable t) {
                                log.error(
                                        "Establishing Resource Manager connection in Task Executor failed",
                                        t);
                            }
                        }
                    });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            onFatalError(failure);
        }

        @Override
        public void onRegistrationRejection(
                String targetAddress, TaskExecutorRegistrationRejection rejection) {
            onFatalError(
                    new FlinkException(
                            String.format(
                                    "The TaskExecutor's registration at the ResourceManager %s has been rejected: %s",
                                    targetAddress, rejection)));
        }
    }

    private final class TaskManagerActionsImpl implements TaskManagerActions {
        private final JobMasterGateway jobMasterGateway;

        private TaskManagerActionsImpl(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = checkNotNull(jobMasterGateway);
        }

        @Override
        public void notifyFatalError(String message, Throwable cause) {
            try {
                log.error(message, cause);
            } catch (Throwable ignored) {
            }

            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        }

        @Override
        public void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
            runAsync(() -> TaskExecutor.this.failTask(executionAttemptID, cause));
        }

        @Override
        public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
            if (taskExecutionState.getExecutionState().isTerminal()) {
                runAsync(
                        () ->
                                unregisterTaskAndNotifyFinalState(
                                        jobMasterGateway, taskExecutionState.getID()));
            } else {
                TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
            }
        }

        @Override
        public void sendState(int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                              Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                              int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
                              boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs,
                              int activeGroupId) {
            log.debug(GROUP_SHARE, "State migration: The task manager received state " + senderId
                    + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfData reconfData = getOrCreateReconfData(activeGroupId);
                        TaskExecutor.this.reconfDataPerActiveGroup.get(activeGroupId);
                        TaskExecutor.this.setNumOfSenders(numOfSenders, reconfData);
                        TaskExecutor.this.setNumOfReceivers(numOfReceivers, reconfData);
                        TaskExecutor.this.saveStateToBeMigrated(state, reconfData);
                        TaskExecutor.this.saveStateForOtherTMsToBeMigrated(stateForOtherTMs, stateNames, reconfData);
                        TaskExecutor.this.setConfigForMultiTMs(stateMigrationInvolvingMultipleTMs, numOfSenderTMs, reconfData);
                        // I could also just have a counter for the senders that have reached out
                        //  however, I keep the list to make sure I never communicate
                        //  with the same sender again before sending the state
                        //  (could happen if a controlMessage is sent before the previous is completed)
                        TaskExecutor.this.saveSender(senderId, reconfData);
                        TaskExecutor.this.tryToSendState(activeGroupId);
                    });
        }

        @Override
        public void sendState(
                int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                Map<Integer, Map<Integer, HashMap<?, ?>>> deduplicationMaps,
                Map<Integer, List<HeapPriorityQueueElement>> triggers,
                Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> deduplicationMapsForOtherTMs,
                Map<ResourceID, Map<Integer, List<byte[]>>> triggersForOtherTMs,
                int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
                boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs,
                int activeGroupId) {
            log.debug(GROUP_SHARE, "State migration: The task manager received windowed state "
                    + senderId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfData reconfData = getOrCreateReconfData(activeGroupId);
                        TaskExecutor.this.setNumOfSenders(numOfSenders, reconfData);
                        TaskExecutor.this.setNumOfReceivers(numOfReceivers, reconfData);
                        TaskExecutor.this.saveStateToBeMigrated(state, reconfData);
                        TaskExecutor.this.saveWindowStateToBeMigrated(deduplicationMaps, triggers, reconfData);
                        TaskExecutor.this.saveStateForOtherTMsToBeMigrated(stateForOtherTMs, stateNames, reconfData);
                        TaskExecutor.this.saveWindowStateForOtherTMsToBeMigrated(
                                deduplicationMapsForOtherTMs, triggersForOtherTMs, reconfData);
                        TaskExecutor.this.setConfigForMultiTMs(stateMigrationInvolvingMultipleTMs,
                                numOfSenderTMs, reconfData);
                        TaskExecutor.this.saveSender(senderId, reconfData);
                        TaskExecutor.this.tryToSendState(activeGroupId);
                    });
        }

        @Override
        public void sendStatePassiveQuery(
                int senderId,
                Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                Map<Integer, String> stateNames,
                int numOfSenders, int numOfReceivers, int activeGroupId) {
            log.debug(GROUP_SHARE, "State migration: The task manager received state from passive query "
                    + senderId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfData reconfData = getOrCreateReconfData(activeGroupId);
                        TaskExecutor.this.setNumOfSenders(numOfSenders, reconfData);
                        TaskExecutor.this.setNumOfReceivers(numOfReceivers, reconfData);
                        TaskExecutor.this.saveStateToBeMigratedPassiveQuery(state, reconfData);
                        TaskExecutor.this.saveStateForOtherTMsToBeMigratedPassive(stateForOtherTMs, stateNames, reconfData);
                        TaskExecutor.this.saveSender(senderId + OFFSET_FOR_SENDER_ID_PASSIVE_QUERY, reconfData);
                        TaskExecutor.this.tryToSendState(activeGroupId);
                    });
        }

        @Override
        public void sendReceiverExecutionAttemptID(
                int receiverId, ExecutionAttemptID executionAttemptID, int numOfSenders,
                int numOfReceivers, boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs,
                int activeGroupId) {
            log.debug(GROUP_SHARE, "Task manager received the execution attempt id of a receiver "
                    + receiverId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfData reconfData = getOrCreateReconfData(activeGroupId);
                        TaskExecutor.this.setNumOfSenders(numOfSenders, reconfData);
                        TaskExecutor.this.setNumOfReceivers(numOfReceivers, reconfData);
                        TaskExecutor.this.setConfigForMultiTMs(stateMigrationInvolvingMultipleTMs,
                                numOfSenderTMs, reconfData);
                        TaskExecutor.this.saveReceiver(receiverId, executionAttemptID, reconfData);
                        TaskExecutor.this.tryToSendState(activeGroupId);
                    });
        }

        @Override
        public void sendStateOfDownstream(int senderId, Map<String, StateMap<?, ?, ?>[]> state,
                                          Map<String, byte[][]> serializedState,
                                          int numOfSenders, ResourceID receiverTMId,
                                          boolean multipleTMsInvolvedInMigrationDownstream,
                                          int numOfSenderTMs, int activeGroupId) {
            log.debug(GROUP_SHARE, "State migration: The task manager received state of downstream "
                    + senderId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfDataDownstream reconfData = getOrCreateReconfDataDownstream(activeGroupId);
                        TaskExecutor.this.setNumOfDownstreamSenders(numOfSenders, reconfData);
                        TaskExecutor.this.saveStateToBeMigratedDownstream(senderId, state, reconfData);
                        TaskExecutor.this.setConfigForMultiTMsDownstream(multipleTMsInvolvedInMigrationDownstream, numOfSenderTMs, reconfData);
                        if (serializedState != null) {
                            TaskExecutor.this.saveStateToBeMigratedDownstreamForOtherTMs(
                                    senderId, serializedState, receiverTMId, reconfData);
                        }
                        TaskExecutor.this.tryToSendStateDownstream(activeGroupId);
                    });
        }

        @Override
        public void sendStateOfDownstream(int senderId, Map<String, StateMap<?, ?, ?>[]> state,
                                          Map<String, byte[][]> serializedState,
                                          HashMap<?, ?>[] deduplicationMaps,
                                          HashMap<byte[], byte[]>[] dedupMapsForOtherTMs,
                                          HeapPriorityQueueElement[] triggers,
                                          byte[][] triggersForOtherTMs, int queueSize,
                                          int numOfSenders, ResourceID receiverTMId,
                                          boolean multipleTMsInvolvedInMigration,
                                          int numOfSenderTMs, int activeGroupId) {
            log.debug(GROUP_SHARE, "State migration: The task manager received state of downstream "
                    + senderId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfDataDownstream reconfData = getOrCreateReconfDataDownstream(activeGroupId);
                        TaskExecutor.this.setNumOfDownstreamSenders(numOfSenders, reconfData);
                        TaskExecutor.this.saveStateToBeMigratedDownstream(senderId, state, reconfData);
                        TaskExecutor.this.saveWindowStateToBeMigratedDownstream(
                                senderId, deduplicationMaps, triggers, queueSize, reconfData);
                        TaskExecutor.this.setConfigForMultiTMsDownstream(multipleTMsInvolvedInMigration, numOfSenderTMs, reconfData);
                        if (serializedState != null) {
                            TaskExecutor.this.saveStateToBeMigratedDownstreamForOtherTMs(
                                    senderId, serializedState, receiverTMId, reconfData);
                            TaskExecutor.this.saveWindowStateToBeMigratedDownstreamForOtherTMs(
                                    senderId, dedupMapsForOtherTMs, triggersForOtherTMs, queueSize, receiverTMId, reconfData);
                        }
                        TaskExecutor.this.tryToSendStateDownstream(activeGroupId);
                    });
        }

        @Override
        public void sendDownstreamReceiverExecutionAttemptID(int receiverId, ExecutionAttemptID executionAttemptID,
                                                   int numOfReceivers, int activeGroupId) {
            log.debug(GROUP_SHARE, "Task manager received the execution attempt id of a downstream receiver "
                    + receiverId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfDataDownstream reconfData = getOrCreateReconfDataDownstream(activeGroupId);
                        TaskExecutor.this.setNumOfDownstreamReceivers(numOfReceivers, reconfData);
                        TaskExecutor.this.saveDownstreamReceiver(receiverId, executionAttemptID, reconfData);
                        TaskExecutor.this.tryToSendStateDownstream(activeGroupId);
                    });
        }

        @Override
        public void sendLastTupleData(int senderId, ReconfigurableSourceData tupleData,
                                      int numOfSources, int numOfTMs, int activeGroupId) {
            log.debug(GROUP_SHARE, "Task manager received info about the last tuple before reconfiguration from "
                    + senderId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfDataSource reconfData = getOrCreateReconfDataSource(activeGroupId);
                        TaskExecutor.this.setNumOfSourcesActive(numOfSources, reconfData);
                        TaskExecutor.this.setNumOfTMsSources(numOfTMs, reconfData);
                        TaskExecutor.this.saveLastTupleData(senderId, tupleData, activeGroupId);
                    });
        }

        @Override
        public void sendSourceTaskExecutionAttemptID(int taskId, ExecutionAttemptID executionAttemptID,
                                                     int numOfSources, int numOfTMs,
                                                     ResourceID taskManagerId, int activeGroupId) {
            log.debug(GROUP_SHARE, "Task manager received the execution attempt id of a source task "
                    + taskId + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        ReconfDataSource reconfData = getOrCreateReconfDataSource(activeGroupId);
                        TaskExecutor.this.setNumOfSourcesPassive(numOfSources, reconfData);
                        TaskExecutor.this.setNumOfTMsSources(numOfTMs, reconfData);
                        TaskExecutor.this.setResourceId(taskManagerId);
                        TaskExecutor.this.saveSourceTask(taskId, executionAttemptID, activeGroupId);
                    });
        }

        @Override
        public void sendDriverJobIDAndHostname(
                String driverJobID, String hostname, String driverPort,
                Set<ResourceID> taskManagerIDsPassiveQuery, int activeGroupId) {
            log.debug(GROUP_SHARE, "Task manager received the driver job id" + " active-id " + activeGroupId);
            runAsync(
                    () ->
                    {
                        TaskExecutor.this.requestNewDriver(
                                driverJobID, hostname, driverPort, taskManagerIDsPassiveQuery, activeGroupId);
                    });
        }
    }

    private class SlotActionsImpl implements SlotActions {

        @Override
        public void freeSlot(final AllocationID allocationId) {
            runAsync(
                    () ->
                            freeSlotInternal(
                                    allocationId,
                                    new FlinkException(
                                            "TaskSlotTable requested freeing the TaskSlot "
                                                    + allocationId
                                                    + '.')));
        }

        @Override
        public void timeoutSlot(final AllocationID allocationId, final UUID ticket) {
            runAsync(() -> TaskExecutor.this.timeoutSlot(allocationId, ticket));
        }
    }

    private class JobManagerHeartbeatListener
            implements HeartbeatListener<
                    AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            validateRunsInMainThread();
            log.info("The heartbeat of JobManager with id {} timed out.", resourceID);

            jobTable.getConnection(resourceID)
                    .ifPresent(
                            jobManagerConnection ->
                                    disconnectAndTryReconnectToJobManager(
                                            jobManagerConnection,
                                            new TimeoutException(
                                                    "The heartbeat of JobManager with id "
                                                            + resourceID
                                                            + " timed out.")));
        }

        @Override
        public void reportPayload(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            validateRunsInMainThread();
            OptionalConsumer.of(jobTable.getConnection(allocatedSlotReport.getJobId()))
                    .ifPresent(
                            jobManagerConnection -> {
                                syncSlotsWithSnapshotFromJobMaster(
                                        jobManagerConnection.getJobManagerGateway(),
                                        allocatedSlotReport);
                            })
                    .ifNotPresent(
                            () ->
                                    log.debug(
                                            "Ignoring allocated slot report from job {} because there is no active leader.",
                                            allocatedSlotReport.getJobId()));
        }

        @Override
        public TaskExecutorToJobManagerHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return jobTable.getConnection(resourceID)
                    .map(
                            jobManagerConnection -> {
                                JobID jobId = jobManagerConnection.getJobId();

                                Set<ExecutionAttemptID> deployedExecutions = new HashSet<>();
                                List<AccumulatorSnapshot> accumulatorSnapshots =
                                        new ArrayList<>(16);
                                Iterator<Task> allTasks = taskSlotTable.getTasks(jobId);

                                while (allTasks.hasNext()) {
                                    Task task = allTasks.next();
                                    deployedExecutions.add(task.getExecutionId());
                                    accumulatorSnapshots.add(
                                            task.getAccumulatorRegistry().getSnapshot());
                                }
                                return new TaskExecutorToJobManagerHeartbeatPayload(
                                        new AccumulatorReport(accumulatorSnapshots),
                                        new ExecutionDeploymentReport(deployedExecutions));
                            })
                    .orElseGet(TaskExecutorToJobManagerHeartbeatPayload::empty);
        }
    }

    private class ResourceManagerHeartbeatListener
            implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            validateRunsInMainThread();
            // first check whether the timeout is still valid
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceId()
                            .equals(resourceId)) {
                log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

                reconnectToResourceManager(
                        new TaskManagerException(
                                String.format(
                                        "The heartbeat of ResourceManager with id %s timed out.",
                                        resourceId)));
            } else {
                log.debug(
                        "Received heartbeat timeout for outdated ResourceManager id {}. Ignoring the timeout.",
                        resourceId);
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return new TaskExecutorHeartbeatPayload(
                    taskSlotTable.createSlotReport(getResourceID()),
                    partitionTracker.createClusterPartitionReport());
        }
    }

    @VisibleForTesting
    static final class TaskExecutorJobServices implements JobTable.JobServices {

        private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

        private final Runnable closeHook;

        private TaskExecutorJobServices(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            this.classLoaderLease = classLoaderLease;
            this.closeHook = closeHook;
        }

        @Override
        public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
            return classLoaderLease;
        }

        @Override
        public void close() {
            classLoaderLease.release();
            closeHook.run();
        }

        @VisibleForTesting
        static TaskExecutorJobServices create(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            return new TaskExecutorJobServices(classLoaderLease, closeHook);
        }
    }

    private class TriggersOfDownstream {
        public HeapPriorityQueueElement[] triggers;
        public int queueSize;

        public TriggersOfDownstream(HeapPriorityQueueElement[] triggers, int queueSize) {
            this.triggers = triggers;
            this.queueSize = queueSize;
        }
    }
}
