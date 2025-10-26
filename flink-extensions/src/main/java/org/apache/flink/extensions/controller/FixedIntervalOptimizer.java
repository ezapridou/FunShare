package org.apache.flink.extensions.controller;

import net.michaelkoepf.spegauge.api.sut.DataRange;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.extensions.controller.ReconfigurationControlMessage.RangeForMigration;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This optimizer reconfigures the query groups using a fixed interval.
 * Disclaimer: This optimizer contains hardcoded logic.
 * E.g. it assumes the active group is group 0. It assumes two queries are involved.
 * It assumes there is enough data for all the iterations to complete.
 * Use this optimizer for testing purposes.
 *
 * This optimizer also works for queries with filters.
 */
public class FixedIntervalOptimizer extends Optimizer {
    private int DOPinIsolation;
    private int iterations;
    private boolean repeated;
    private int initialDelay;
    private int interval;

    public void init(String jobID,
                     HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping,
                     HashMap<String, ExecutionVertex> operatorToExecutionVertexMap,
                     HashMap<String, ArrayList<ExecutionJobVertex>> targetVertices,
                     HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers,
                     HashMap<String, ArrayList<Integer>> queryCategories,
                     HashMap<Integer, DataRange> filtersPerQuery,
                     HashMap<Integer, HashSet<String>> sources,
                     int DOPinIsolation, int DOPmax, int driverPort,
                     int iterations, boolean repeated, int initialDelay, int interval) {
        init(jobID, graphWithMapping, targetWorkers, queryCategories, filtersPerQuery, sources,
                DOPmax, driverPort);
        this.DOPinIsolation = DOPinIsolation;
        this.iterations = iterations;
        this.repeated = repeated;
        this.initialDelay = initialDelay;
        this.interval = interval;
        start(operatorToExecutionVertexMap, targetVertices);
    }

    @Override
    public void start(Map<String, ExecutionVertex> operatorToExecutionVertexMap,
                      Map<String, ArrayList<ExecutionJobVertex>> targetVertices) {

        int numOfQueries = getNumOfQueries();
        int sharingDOP = DOPinIsolation * numOfQueries;

        int activeGroup = 0;
        int passiveGroup = 1;

        int activeQuery = 0;
        int passiveQuery = 1;

        boolean[] querySetActive = new boolean[numOfQueries];
        boolean[] querySetPassive = new boolean[numOfQueries];
        querySetActive[activeQuery] = true;
        querySetPassive[passiveQuery] = true;

        ArrayList<Integer> activeChannelsInIsolation = new ArrayList<>();
        ArrayList<Integer> activeChannelsShared = new ArrayList<>();

        for (int i = 0; i < DOPinIsolation; i++) {
            activeChannelsInIsolation.add(i);
            activeChannelsShared.add(i);
        }
        for (int i = DOPinIsolation; i < sharingDOP; i++) {
            activeChannelsShared.add(i);
        }

        String queryCategory = targetWorkers.keySet().iterator().next();

        Timer t = new Timer();
        TimerTask task = new TimerTask() {
            private int iteration = 0;

            @Override
            public void run() {
                int currentIteration = iteration;
                iteration++;

                if (currentIteration >= iterations) {
                    this.cancel();
                }

                List<CompletableFuture<?>> futures1 = new ArrayList<>();
                List<Long> roundtripTimes1 = new ArrayList<>();

                List<CompletableFuture<?>> futures2 = new ArrayList<>();
                List<Long> roundtripTimes2 = new ArrayList<>();

                MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationTypePassiveQuery = MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION;
                RangeForMigration rangeForMigration = null;

                // map from operator name to TaskManagerLocation
                HashMap<String, ResourceID> taskToResourceIDMap = getTaskToResourceIDMap(operatorToExecutionVertexMap);

                if (currentIteration == 0) {
                    // change parallelism of active group
                    int oldDOP = sharingDOP;
                    int newDOP = DOPinIsolation;
                    ArrayList<Integer> oldActiveChannels = new ArrayList<>(oldDOP);
                    for (int i = 0; i < oldDOP; i++) {
                        oldActiveChannels.add(i);
                    }
                    ControlMessage controlMessage = getControlMessageForChangingDOP(oldDOP, newDOP, activeGroup,
                            queryCategory, taskToResourceIDMap, oldActiveChannels, activeChannelsInIsolation, false);
                    sendControlMessage(controlMessage, activeGroup, operatorToExecutionVertexMap, futures1, roundtripTimes1);
                } else if (currentIteration < iterations) {
                    int queryToBeMoved = passiveQuery;
                    Set<Integer> queriesToBeMoved = new HashSet<>(1);
                    queriesToBeMoved.add(queryToBeMoved);

                    int oldDOPActive, newDOPActive, oldDOPPassive, newDOPPassive;
                    boolean share;
                    ArrayList<Integer> activeChannelsActiveGroup;
                    ArrayList<Integer> activeChannelsPassiveGroup;
                    ArrayList<Integer> oldActiveChannelsActive;
                    ArrayList<Integer> oldActiveChannelsPassive;

                    // share
                    if (currentIteration % 2 == 1) {
                        oldDOPActive = DOPinIsolation;
                        newDOPActive = sharingDOP;
                        oldDOPPassive = DOPinIsolation;
                        newDOPPassive = DOPinIsolation;
                        share = true;
                        oldActiveChannelsActive = activeChannelsInIsolation;
                        activeChannelsActiveGroup = activeChannelsShared;
                        activeChannelsPassiveGroup = null;
                        oldActiveChannelsPassive = activeChannelsInIsolation;

                        migrationTypePassiveQuery = getMigrationTypeFromPassive(activeQuery, passiveQuery, queryCategory);

                        if (migrationTypePassiveQuery == MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE) {
                            rangeForMigration = getRangeForMigration(activeQuery, passiveQuery);
                        }
                    }
                    // split
                    else {
                        oldDOPActive = sharingDOP;
                        newDOPActive = DOPinIsolation;
                        oldDOPPassive = DOPinIsolation;
                        newDOPPassive = DOPinIsolation;
                        share = false;
                        oldActiveChannelsActive = activeChannelsShared;
                        activeChannelsActiveGroup = activeChannelsInIsolation;
                        activeChannelsPassiveGroup = activeChannelsInIsolation;
                        oldActiveChannelsPassive = null;
                    }
                    // only one passive group
                    HashMap<Integer, Integer> oldDOPsPassive = new HashMap<>();
                    oldDOPsPassive.put(passiveQuery, oldDOPPassive);
                    HashSet<Integer> passiveGroups = new HashSet<>();
                    passiveGroups.add(passiveGroup);
                    HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypesPassiveGroups = new HashMap<>();
                    migrationTypesPassiveGroups.put(passiveQuery, migrationTypePassiveQuery);
                    HashMap<Integer, ArrayList<Integer>> oldActiveChannelsPassiveGroups = new HashMap<>();
                    oldActiveChannelsPassiveGroups.put(passiveQuery, oldActiveChannelsPassive);

                    ControlMessage controlMessageActiveGroup = getControlMessageForActiveGroup(
                            oldDOPActive, newDOPActive, activeGroup,
                            passiveGroups, queryCategory, queriesToBeMoved, share, querySetActive,
                            migrationTypesPassiveGroups, taskToResourceIDMap, oldActiveChannelsActive,
                            activeChannelsActiveGroup, oldActiveChannelsPassiveGroups);
                    sendControlMessage(controlMessageActiveGroup, activeGroup, operatorToExecutionVertexMap, futures1, roundtripTimes1);
                    ControlMessage controlMessagePassiveGroup = getControlMessageForPassiveGroup(
                            oldDOPActive, newDOPActive, oldDOPPassive, newDOPPassive, activeGroup,
                            passiveGroup, passiveGroups, queryCategory, queriesToBeMoved, share,
                            querySetPassive,
                            migrationTypePassiveQuery, rangeForMigration, taskToResourceIDMap,
                            oldActiveChannelsActive,
                            activeChannelsActiveGroup, activeChannelsPassiveGroup, oldActiveChannelsPassive);
                    sendControlMessage(controlMessagePassiveGroup, passiveGroup, operatorToExecutionVertexMap, futures2, roundtripTimes2);
                }

                printRoundTripTime(futures1, roundtripTimes1, "[Active Group]");
                if (currentIteration > 0 && currentIteration < iterations) {
                    printRoundTripTime(futures2, roundtripTimes2, "[Passive Group]");
                }
                System.out.println("Job " + jobID + " sent iteration " + currentIteration + " time=" + System.currentTimeMillis());
            }
        };

        if (!targetWorkers.isEmpty()) {
            if (repeated) {
                t.schedule(task, initialDelay, interval);
            } else {
                t.schedule(task, initialDelay);
            }
        }
    }
}
