package org.apache.flink.extensions.reconfiguration;

import java.util.Iterator;

/**
 * Interface that makes contained method available to the controller package.
 * TODO: We could also use reflection to call the methods, but I need to check how that works in Scala.
 */
public interface IConsumerVertexGroup {
    Iterator<String> getAllWorkerNames();
}
