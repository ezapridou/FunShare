package org.apache.flink.extensions.controller;

public enum MIGRATION_TYPE_FROM_PASSIVE_QUERY implements java.io.Serializable{
    NO_MIGRATION,
    KEY_RANGE,
    ENTIRE_STATE;
}
