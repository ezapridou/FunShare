package org.apache.flink.extensions.controller;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

public class ReconfigurationControlMessage extends ControlMessage {
    public static class RangeForMigration implements Serializable {
        public final long start;
        public final long end;
        public final long start2;
        public final long end2;

        public RangeForMigration(long start, long end, long start2, long end2) {
            this.start = start;
            this.end = end;
            this.start2 = start2;
            this.end2 = end2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RangeForMigration that = (RangeForMigration) o;
            return start == that.start &&
                    end == that.end &&
                    start2 == that.start2 &&
                    end2 == that.end2;
        }

        @Override
        public int hashCode() {
            int result = 31;
            result = 31 * result + (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            result = 31 * result + (int) (start2 ^ (start2 >>> 32));
            result = 31 * result + (int) (end2 ^ (end2 >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "RangeForMigration(" +
                    "start=" + start +
                    ", end=" + end +
                    ", start2=" + start2 +
                    ", end2=" + end2 +
                    ')';
        }
    }
    private static final long serialVersionUID = 2L;

    public final HashSet<String> senders;
    public final HashSet<String> receivers;
    public final HashMap<ResourceID, HashMap<String, Integer>> numOfSenders;
    public final HashMap<ResourceID, HashMap<String, Integer>> numOfReceivers;
    // only needed for the control message of the passive query
    public final MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationType;
    // only needed for the control message of the passive query
    public final RangeForMigration rangeForMigration;
    // only needed for the control message of the passive query
    public final HashSet<String> sendersOfPassiveQuery;
    public final HashSet<String> upstreamTasks;
    public final HashSet<String> tasksToChangeInputStatus;
    public final HashSet<String> downstreamSenders;
    public final HashSet<String> downstreamReceivers;
    public final HashMap<ResourceID, HashSet<String>> sourcesOfActiveQuery;
    public final HashMap<ResourceID, HashSet<String>> sourcesOfPassiveQuery;
    public final HashMap<String, ResourceID> sourceToResourceIDMap;
    public final boolean[] querySet;
    public final int oldDOPActive;
    public final int newDOPActive;
    public final int maxParallelism;
    // only needed for the control message of the passive query
    public final int oldDOPPassive;
    public final int newDOPPassive;
    public final boolean share;
    public final int driverPort;
    public final HashMap<String, ResourceID> taskToResourceIDMap;
    public final HashMap<Integer, ResourceID> partitionToResourceIDMap;
    public final HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamSenders;
    public final HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamReceivers;
    // only needed for the control message of the passive query
    public final HashMap<Integer, ResourceID> partitionToResourceIDMapPassive;
    public final int numberOfSenderTMsInvolvedInMigration;
    public final boolean multipleTMsInvolvedInMigration;
    public final int numberOfSenderTMsInvolvedInMigrationDownstream;
    public final boolean multipleTMsInvolvedInMigrationDownstream;
    public final boolean isGroupActive;
    public final ArrayList<Integer> activeChannelsOfActiveGroup;
    // only needed for the control message of the passive query
    public final ArrayList<Integer> activeChannelsOfPassiveGroup;
    public final int activeGroupId;
    public final boolean enableSplitMonitoring;

    public ReconfigurationControlMessage(
            HashMap<String, HashSet<String>> MCS, HashSet<String> senders, HashSet<String> receivers,
            HashMap<ResourceID, HashMap<String, Integer>> numOfSenders,
            HashMap<ResourceID, HashMap<String, Integer>> numOfReceivers,
            MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationType, RangeForMigration rangeForMigration,
            HashSet<String> sendersOfPassiveQuery, HashSet<String> upstreamTasks,
            HashSet<String> tasksToChangeInputStatus, HashSet<String> downstreamSenders,
            HashSet<String> downstreamReceivers,
            HashMap<ResourceID, HashSet<String>> sourcesOfActiveQuery,
            HashMap<ResourceID, HashSet<String>> sourcesOfPassiveQuery,
            HashMap<String, ResourceID> sourceToResourceIDMap,
            boolean[] querySet, int oldDOPActive, int newDOPActive, int maxParallelism,
            int oldDOPPassive, int newDOPPassive, boolean share, int driverPort,
            HashMap<String, ResourceID> taskToResourceIDMap,
            HashMap<Integer, ResourceID> partitionToResourceIDMap,
            HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamSenders,
            HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamReceivers,
            HashMap<Integer, ResourceID> partitionToResourceIDMapPassive,
            int numberOfSenderTMsInvolvedInMigration, boolean multipleTMsInvolvedInMigration,
            int numberOfSenderTMsInvolvedInMigrationDownstream,
            boolean multipleTMsInvolvedInMigrationDownstream, boolean isGroupActive,
            ArrayList<Integer> activeChannelsOfActiveGroup,
            ArrayList<Integer> activeChannelsOfPassiveGroup, int activeGroupId,
            boolean enableSplitMonitoring) {

        super(MCS);
        this.senders = senders;
        this.receivers = receivers;
        this.numOfSenders = numOfSenders;
        this.numOfReceivers = numOfReceivers;
        this.migrationType = migrationType;
        this.rangeForMigration = rangeForMigration;
        this.sendersOfPassiveQuery = sendersOfPassiveQuery;
        this.upstreamTasks = upstreamTasks;
        this.tasksToChangeInputStatus = tasksToChangeInputStatus;
        this.downstreamSenders = downstreamSenders;
        this.downstreamReceivers = downstreamReceivers;
        this.sourcesOfActiveQuery = sourcesOfActiveQuery;
        this.sourcesOfPassiveQuery = sourcesOfPassiveQuery;
        this.sourceToResourceIDMap = sourceToResourceIDMap;
        this.querySet = querySet;
        this.oldDOPActive = oldDOPActive;
        this.newDOPActive = newDOPActive;
        this.maxParallelism = maxParallelism;
        this.oldDOPPassive = oldDOPPassive;
        this.newDOPPassive = newDOPPassive;
        this.share = share;
        this.driverPort = driverPort;
        this.taskToResourceIDMap = taskToResourceIDMap;
        this.partitionToResourceIDMap = partitionToResourceIDMap;
        this.partitionToResourceIDMapDownstreamSenders = partitionToResourceIDMapDownstreamSenders;
        this.partitionToResourceIDMapDownstreamReceivers = partitionToResourceIDMapDownstreamReceivers;
        this.partitionToResourceIDMapPassive = partitionToResourceIDMapPassive;
        this.numberOfSenderTMsInvolvedInMigration = numberOfSenderTMsInvolvedInMigration;
        this.multipleTMsInvolvedInMigration = multipleTMsInvolvedInMigration;
        this.numberOfSenderTMsInvolvedInMigrationDownstream = numberOfSenderTMsInvolvedInMigrationDownstream;
        this.multipleTMsInvolvedInMigrationDownstream = multipleTMsInvolvedInMigrationDownstream;
        this.isGroupActive = isGroupActive;
        this.activeChannelsOfActiveGroup = activeChannelsOfActiveGroup;
        this.activeChannelsOfPassiveGroup = activeChannelsOfPassiveGroup;
        this.activeGroupId = activeGroupId;
        this.enableSplitMonitoring = enableSplitMonitoring;
    }
}
