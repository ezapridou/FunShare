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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and emits records to the
 * channel selected by the {@link ChannelSelector} for regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable>
        extends RecordWriter<T> {

    private final ChannelSelector<T> channelSelector;
    private int totalNumberOfChannels;
    private List<Integer> indexToChannel;

    ChannelSelectorRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector<T> channelSelector,
            long timeout,
            String taskName) {
        super(writer, timeout, taskName);

        this.channelSelector = checkNotNull(channelSelector);
        this.channelSelector.setup(numberOfChannels);
        this.totalNumberOfChannels = numberOfChannels;
        this.indexToChannel = new ArrayList<>(numberOfChannels);
        for (int i = 0; i < numberOfChannels; i++) {
            indexToChannel.add(i);
        }
    }

    @Override
    public void emit(T record) throws IOException {
        int result = channelSelector.selectChannel(record);
        int channel;
        try {
            channel = indexToChannel.get(result);
        } catch (Exception e) {
            System.out.println("Error in selecting channel: " + result + " indexToChannel: " + indexToChannel + " totalNumberOfChannels: " + totalNumberOfChannels + " numberOfChannels: " + numberOfChannels);
            throw e;
        }
        //System.out.println(Thread.currentThread().getName()+" push record "+((SerializationDelegate<?>)record).getInstance()+" with selection = "+result);
        emit(record, channel);
    }

    @Override
    public void broadcastEmit(T record) throws IOException {
        checkErroneous();
        // Emitting to all channels in a for loop can be better than calling
        // ResultPartitionWriter#broadcastRecord because the broadcastRecord
        // method incurs extra overhead.
        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        // GroupShare debug print for watermark broadcasting
        // System.out.println(Thread.currentThread().getName()+" broadcast record "+((SerializationDelegate<?>)record).getInstance());
        // TODO GroupShare alternative implementation for correct watermark propagation while reconfiguring the query plans
        //for (int channelIndex = 0; channelIndex < totalNumberOfChannels; channelIndex++) {
        for (int channelIndex = 0; channelIndex < numberOfChannels; channelIndex++) {
            serializedRecord.rewind();
            emit(record, indexToChannel.get(channelIndex));
        }

        if (flushAlways) {
            flushAll();
        }
    }

    // This method is used for debugging purposes
    public String getChannelSelectorClass() {
        return channelSelector.getClass().getName();
    }

    @Override
    public void setNumberOfChannels(int numberOfChannels, List<Integer> indexToChannel) {
        if (numberOfChannels > totalNumberOfChannels) {
            throw new IllegalArgumentException(
                    "The number of channels cannot exceed the total number of channels.");
        }
        if (indexToChannel == null) {
            // no new active channels so we will just keep the first numberOfChannels channels
            setNumberOfChannels(numberOfChannels);
        }
        channelSelector.setNumberOfChannels(numberOfChannels);
        this.numberOfChannels = numberOfChannels;
        this.indexToChannel = indexToChannel;
    }

    private void setNumberOfChannels(int numberOfChannels) {
        int oldNumberOfChannels = this.numberOfChannels;
        assert oldNumberOfChannels >= numberOfChannels;
        channelSelector.setNumberOfChannels(numberOfChannels);
        this.numberOfChannels = numberOfChannels;
        // remove the last oldNumberOfChannels - numberOfChannels channels
        for (int i = oldNumberOfChannels - 1; i >= numberOfChannels; i--) {
            indexToChannel.remove(i);
        }
    }

    public boolean hasForwardPartitioner() {
        return channelSelector.toString().equals("FORWARD");
    }
}
