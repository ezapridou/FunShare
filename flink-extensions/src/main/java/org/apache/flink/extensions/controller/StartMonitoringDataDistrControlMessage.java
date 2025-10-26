package org.apache.flink.extensions.controller;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;

import net.michaelkoepf.spegauge.api.sut.DataRange;

public class StartMonitoringDataDistrControlMessage extends ControlMessage {
    private static final long serialVersionUID = 3L;
    public final HashMap<Integer, HashSet<Integer>> queryToDataRangesMap;
    public final DataRange[] dataRanges;
    public final HashSet<Integer> activeQueries;

    public StartMonitoringDataDistrControlMessage(
            HashMap<String, HashSet<String>> MCS,
            HashMap<Integer, HashSet<Integer>> queryToDataRangesMap,
            DataRange[] dataRanges, HashSet<Integer> activeQueries) {
        super(MCS);
        this.queryToDataRangesMap = queryToDataRangesMap;
        this.dataRanges = dataRanges;
        this.activeQueries = activeQueries;
    }
}
