package org.apache.flink.extensions.controller;

import java.util.HashMap;
import java.util.HashSet;

public class StopMonitoringDataDistrControlMessage extends ControlMessage{
    public enum Type {
        FILTER, JOIN
    }

    private static final long serialVersionUID = 4L;
    private Type type;
    public StopMonitoringDataDistrControlMessage(
            HashMap<String, HashSet<String>> MCS, Type type) {
        super(MCS);
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}
