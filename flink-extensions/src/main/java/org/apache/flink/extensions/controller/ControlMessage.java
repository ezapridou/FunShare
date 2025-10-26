package org.apache.flink.extensions.controller;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

public abstract class ControlMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final long FixedEpochNumber = 999999999L;

    public final HashMap<String, HashSet<String>> MCS;

    // Static utility methods
    public static byte[] serialize(ControlMessage obj) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
        try {
            objOut.writeObject(obj);
            return byteOut.toByteArray();
        } finally {
            objOut.close();
            byteOut.close();
        }
    }

    public static ControlMessage deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        ObjectInputStream objIn = new ObjectInputStream(byteIn);
        try {
            return (ControlMessage) objIn.readObject();
        } finally {
            objIn.close();
            byteIn.close();
        }
    }

    public ControlMessage(HashMap<String, HashSet<String>> MCS) {
        this.MCS = MCS;
    }
}
