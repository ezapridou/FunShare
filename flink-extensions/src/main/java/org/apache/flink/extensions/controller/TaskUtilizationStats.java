package org.apache.flink.extensions.controller;

import java.io.Serializable;

public class TaskUtilizationStats implements Serializable {
    public long idleTimeMsPerSecond;
    public long backPressuredTimeMsPerSecond;

    public double busyTimeMsPerSecond;


    public TaskUtilizationStats(
            long idleTimeMsPerSecond, long backPressuredTimeMsPerSecond, double busyTimeMsPerSecond) {
        this.idleTimeMsPerSecond = idleTimeMsPerSecond;
        this.backPressuredTimeMsPerSecond = backPressuredTimeMsPerSecond;
        this.busyTimeMsPerSecond = busyTimeMsPerSecond;
    }

    public String toString() {
        return "TaskUtilizationStats{" +
                "idleTimeMsPerSecond=" + idleTimeMsPerSecond +
                ", backPressuredTimeMsPerSecond=" + backPressuredTimeMsPerSecond +
                ", busyTimeMsPerSecond=" + busyTimeMsPerSecond +
                '}';
    }
}
