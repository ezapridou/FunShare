package org.apache.flink.extensions.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.sut.DataRange;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.extensions.reconfiguration.IConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;

import java.util.*;

public class Controller {

    private final boolean repeated;
    private final int initialDelay;
    private final int optimizationStartDelay;
    private final int interval;
    private final String targetsJSON;
    private final String oneToManyOperators;
    private final String scheduler;
    private final int DOPinIsolation;
    private final int DOPmax;
    private final int iterations;
    private final int driverPort;
    private final String optimizerName;
    private final String queriesPerCategoryStr;
    private final String filtersPerQueryStr;

    private final int inputRate;
    private final int jobCount = 0;

    public Controller() {
        this.repeated = Boolean.parseBoolean(System.getProperty("repeatedReconfiguration", "false"));
        this.initialDelay = Integer.parseInt(System.getProperty("reconfInitialDelay", "5000"));
        this.optimizationStartDelay = Integer.parseInt(System.getProperty("optimizationStartDelay", "5000"));
        this.interval = Integer.parseInt(System.getProperty("reconfInterval", "10000"));
        this.targetsJSON = System.getProperty("reconfTargets", "");
        this.oneToManyOperators = System.getProperty("oneToMany", "");
        this.scheduler = System.getProperty("reconfScheduler", "epoch");
        this.DOPinIsolation = Integer.parseInt(System.getProperty("DOPinIsolation", "2"));
        this.DOPmax = Integer.parseInt(System.getProperty("DOPmax", "4"));
        this.iterations = Integer.parseInt(System.getProperty("iterations", "5"));
        this.driverPort = Integer.parseInt(System.getProperty("driverPort", "8100"));
        this.optimizerName = System.getProperty("optimizer", "FunShareOptimizer");
        this.queriesPerCategoryStr = System.getProperty("queriesPerCategory", "");
        this.filtersPerQueryStr = System.getProperty("filtersPerQuery", "");
        this.inputRate = Integer.parseInt(System.getProperty("inputRate", "0"));
    }

    // Note the bellow does not work with the Fries algorithm. However, for the queries we are running,
    // epoch and Fries would be identical.
    public void registerJobToSendControl(ExecutionGraph graph, JobID jobID) {
        String jobIDStr = jobID.toString();

        ObjectMapper mapper = new ObjectMapper();

        Map<String, List<String>> targetsMap = jsonToMapStringList(mapper, targetsJSON.replace(",", ", "));
        HashMap<String, ArrayList<Integer>> queryCategoriesMap = jsonToMapIntList(mapper, queriesPerCategoryStr.replace(",", ", "));
        HashMap<Integer, DataRange> filtersMap = parseFilters(mapper, filtersPerQueryStr.replace(",", ", "));

        System.out.println("Parameters: repeated: " + repeated + ", initialDelay: " + initialDelay
                + ", optimizationStartDelay: " + optimizationStartDelay + ", interval: " + interval
                + ", targets: " + targetsMap + ", scheduler: " + scheduler
                + ", DOPinIsolation: " + DOPinIsolation + ", DOPmax: " + DOPmax + ", iterations: "
                + iterations + ", driverPort: " + driverPort + ", optimizer: " + optimizerName
                + ", queriesPerCategory: " + queryCategoriesMap + ", filtersPerQuery: " + filtersMap
                + ", inputRate: " + inputRate);

        Pair<HashMap<Integer, HashMap<String, HashSet<String>>>, HashMap<String, ExecutionVertex>> graphAndMapping =
                convertExecutionGraphToWorkerGraph(graph);
        HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping = graphAndMapping.getFirst();
        HashMap<String, ExecutionVertex> operatorToExecutionVertexMap = graphAndMapping.getSecond();

        Pair<HashMap<String, ArrayList<ExecutionJobVertex>>, HashMap<String, HashMap<Integer, HashSet<String>>>> verticesAndWorkers =
                getVerticesAndWorkers(graph, targetsMap);
        HashMap<String, ArrayList<ExecutionJobVertex>> targetVertices = verticesAndWorkers.getFirst();
        HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers = verticesAndWorkers.getSecond();

        HashMap<Integer, HashSet<String>> sources = getSources(graphWithMapping);

        //System.out.println("graph: " + graphWithMapping)
        System.out.println("targets: " + targetWorkers);
        //System.out.println("sources: " + sources);

        // Unused code from Fries
        /*
        val oneToManyOperatorsList: List[String] = oneToManyOperators.split(",").toList
        val (_, oneToManyWorkers) = getVerticesAndWorkers(graph, oneToManyOperatorsList)
        println("oneToMany: " + oneToManyWorkers.mkString(" "))
        val MCS = if (scheduler != "epoch") {
          val result = new util.HashMap[Integer, util.HashMap[String, util.HashSet[String]]]()
          for (queryGraph <- graphWithMapping._1) {
            result.put(queryGraph._1, FriesAlg.computeMCS(queryGraph._2, targetWorkers, oneToManyWorkers))
            println("Query " + queryGraph._1 + "MCS: " + result)
          }
          result
        } else {
          graphWithMapping._1
        }
        */

        if (targetWorkers.isEmpty()) {
            System.out.println("No target workers specified. No optimizer will be instantiated.");
            return;
        }

        switch (optimizerName) {
            case "FixedIntervalOptimizer":
                FixedIntervalOptimizer optimizer = new FixedIntervalOptimizer();
                optimizer.init(jobIDStr, graphWithMapping, operatorToExecutionVertexMap,
                        targetVertices, targetWorkers, queryCategoriesMap, filtersMap, sources, DOPinIsolation,
                        DOPmax, driverPort, iterations, repeated, initialDelay, interval);
                break;
            case "FullShareOptimizer":
                FullShareOptimizer fullShareOptimizer = new FullShareOptimizer();
                fullShareOptimizer.init(jobIDStr, graphWithMapping, operatorToExecutionVertexMap,
                        targetVertices, targetWorkers, queryCategoriesMap, filtersMap, sources, DOPinIsolation,
                        DOPmax, driverPort, iterations, repeated, initialDelay, interval);
                break;
            case "FunShareOptimizer":
                FunShareOptimizer funShareOptimizer = new FunShareOptimizer();
                funShareOptimizer.init(jobIDStr, graphWithMapping, operatorToExecutionVertexMap,
                        targetVertices, targetWorkers, queryCategoriesMap, filtersMap, sources, DOPinIsolation,
                        DOPmax, driverPort, initialDelay, optimizationStartDelay, interval, inputRate);
                break;
            default:
                throw new IllegalArgumentException("Optimizer " + optimizerName + " not supported");
        }
    }

    private int getGroupIdFromName(String name) {
        String[] temp = name.split("GID");
        String[] temp2 = temp[temp.length - 1].split("-");
        return Integer.parseInt(temp2[0].replace(" ", ""));
    }

    private Pair<HashMap<Integer, HashMap<String, HashSet<String>>>, HashMap<String, ExecutionVertex>>
    convertExecutionGraphToWorkerGraph(ExecutionGraph graph) {
        HashMap<Integer, HashMap<String, HashSet<String>>> result = new HashMap<>();
        HashMap<String, ExecutionVertex> mapping = new HashMap<>();

        for (ExecutionJobVertex v : graph.getAllVertices().values()) {
            for (ExecutionVertex w : v.getTaskVertices()) {
                int groupId = getGroupIdFromName(w.getTaskName());
                result.putIfAbsent(groupId, new HashMap<>());
                result.get(groupId).put(
                        w.getTaskName() + "-" + w.getParallelSubtaskIndex(), getAllDownstreamWorkers(w));
                mapping.put(w.getTaskName() + "-" + w.getParallelSubtaskIndex(), w);
            }
        }

        return new Pair<>(result, mapping);
    }

    private HashSet<String> getAllDownstreamWorkers(ExecutionVertex v) {
        HashSet<String> result = new HashSet<>();
        for (IntermediateResultPartition partitions : v.getProducedPartitions().values()) {
            for (ConsumerVertexGroup group : partitions.getConsumerVertexGroups()) {
                for (Iterator<String> it = ((IConsumerVertexGroup) group).getAllWorkerNames(); it.hasNext(); ) {
                    String name = it.next();
                    result.add(name);
                }
            }
        }
        return result;
    }

    private Pair<HashMap<String, ArrayList<ExecutionJobVertex>>, HashMap<String, HashMap<Integer, HashSet<String>>>>
    getVerticesAndWorkers(ExecutionGraph graph, Map<String, List<String>> names) {
        if (names.isEmpty()) {
            return new Pair<>(new HashMap<>(), new HashMap<>());
        }

        HashMap<String, ArrayList<ExecutionJobVertex>> vertices = new HashMap<>();
        HashMap<String, HashMap<Integer, HashSet<String>>> workers = new HashMap<>();

        for (ExecutionJobVertex vertex : graph.getVerticesTopologically()) {
            String v_processed = vertex.getName().toLowerCase().replace(" ", "")
                    .replace("=", "_").replace("->", "");
            //System.out.println("v_processed: " + v_processed);

            for (Map.Entry<String, List<String>> entry : names.entrySet()) {
                String key = entry.getKey();
                List<String> value = entry.getValue();

                if (value.stream().anyMatch(v_processed::equals)) {
                    vertices.computeIfAbsent(key, k -> new ArrayList<>()).add(vertex);

                    int groupId = getGroupIdFromName(vertex.getName());
                    workers.computeIfAbsent(key, k -> new HashMap<>()).computeIfAbsent(groupId, k -> new HashSet<>());

                    for (ExecutionVertex w : vertex.getTaskVertices()) {
                        workers.get(key).get(groupId).add(w.getTaskName() + "-" + w.getParallelSubtaskIndex());
                    }
                }
            }
        }

        return new Pair<>(vertices, workers);
    }

    private HashMap<Integer, HashSet<String>> getSources(Map<Integer, HashMap<String, HashSet<String>>> graphWithMapping) {
        HashMap<Integer, HashSet<String>> result = new HashMap<>();

        for (Map.Entry<Integer, HashMap<String, HashSet<String>>> entry : graphWithMapping.entrySet()) {
            Integer queryId = entry.getKey();
            Map<String, HashSet<String>> queryGraph = entry.getValue();

            result.put(queryId, new HashSet<>());

            for (String vertex : queryGraph.keySet()) {
                if (vertex.startsWith("Source")) {
                    result.get(queryId).add(vertex);
                }
            }
        }

        return result;
    }

    private Map<String, List<String>> jsonToMapStringList(ObjectMapper mapper, String jsonString) {
        try {
            Map<String, List<String>> map = new HashMap<>();
            if (jsonString.isEmpty()) {
                return map;
            }
            JsonNode jsonNode = mapper.readTree(jsonString);

            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                List<String> values = new ArrayList<>();
                entry.getValue().forEach(node -> values.add(node.asText()));
                map.put(entry.getKey(), values);
            }
            return map;
        } catch (Exception e) {
            System.out.println("Error parsing JSON with string " + jsonString + ": " + e);
            return new HashMap<>();
        }
    }

    private HashMap<String, ArrayList<Integer>> jsonToMapIntList(ObjectMapper mapper, String jsonString) {
        try {
            HashMap<String, ArrayList<Integer>> map = new HashMap<>();
            if (jsonString.isEmpty()) {
                return map;
            }
            JsonNode jsonNode = mapper.readTree(jsonString);

            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                ArrayList<Integer> values = new ArrayList<>();
                entry.getValue().forEach(node -> values.add(node.asInt()));
                map.put(entry.getKey(), values);
            }
            return map;
        } catch (Exception e) {
            System.out.println("Error parsing JSON with string " + jsonString + ": " + e);
            return new HashMap<>();
        }
    }

    private HashMap<Integer, DataRange> parseFilters(ObjectMapper mapper, String jsonString) {
        try {
            HashMap<Integer, DataRange> map = new HashMap<>();
            if (jsonString.isEmpty()) {
                return map;
            }
            JsonNode jsonNode = mapper.readTree(jsonString);

            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                long val1 = 0;
                long val2 = 0;
                int i=0;
                for (JsonNode node : entry.getValue()) {
                    if (i == 0) {
                        val1 = node.asLong();
                    } else {
                        val2 = node.asLong();
                    }
                    i++;
                }
                if (val1 < val2) {
                    map.put(Integer.parseInt(entry.getKey()), new DataRange(val1, val2));
                }
                else {
                    map.put(Integer.parseInt(entry.getKey()), new DataRange(val2, val1));
                }
            }
            return map;
        } catch (Exception e) {
            System.out.println("Error parsing JSON with string " + jsonString + ": " + e);
            return new HashMap<>();
        }
    }

    // Helper class for returning pairs of values
    private static class Pair<T, U> {
        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }
}
