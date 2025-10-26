# FunShare on Apache Flink
This is the code for the implementation of FunShare on Apache Flink 1.13. More details about Flink and how to build it can be found at [Flink's github](https://github.com/apache/flink).

## Building the project
Same as for Apache Flink. 
```console
mvn clean package -Drat.numUnapprovedLicenses=1000 -DskipTests -Dfast -Dcheckstyle.skip
```
## Running the project:

This step is the same as Flink's original instructions. FunShare is already a built-in functionality of Flink in this repo. To enable FunShare, you can pass the following parameters as JVM params(start with"-D") in flink-conf.yaml

## FunShare parameters:

Some parameters of FunShare can be tuned:
| Parameter name  | Type | Default value | Usage |
| ------------- | ------------- |  ------------- |  ------------- |
| optimizerName  | String      |  "FixedIntervalOptimizer"  | The optimizer to be used for choosing query groups. Options: FunShareOptimizer, FixedIntervalOptimizer, FullShareOptimizer |
| repeatedReconfiguration  | Boolean      |  false  | repeat same reconfiguration after a given time interval (used for FullShareOptimizer and FixedIntervalOptimizer) | 
| reconfInitialDelay  | Long  |  5000  | Delay of the first reconfiguration (ms) |
| optimizationStartDelay  | Long  |  5000  | Delay of the first optimization step (ms) |
| reconfInterval  | Long  |  10000  | Interval between 2 reconfigurations (ms)| 
| reconfTargets  | JSON      |  ""  |  The JSON contains for each query category which operators can be shared | 
| DOPinIsolation  | Integer      |  2  | Degree of parallelism for isolated execution |
| DOPmax  | Integer      |  4  | Max degree of parallelism for shared execution |
| iterations  | Integer      |  5  | Number of iterations for the job (used for FullShareOptimizer and FixedIntervalOptimizer) |
| driverPort  | Integer      |  8100  | Port number for the data generation driver. |
| queriesPerCategory  | JSON      |  ""  | The queries that belong to each query category |
| filtersPerQuery  | JSON      |  ""  | For each query, its filter specified in a range. |

## Example of the parameters passed via the conf.yaml file:
This example is for 2 2-way-join queries. Query 0 has a range filter [0, 49] and query 1 has a range filter [0, 99].
```console
env.java.opts: "-Doptimizer=FixedIntervalOptimizer -DrepeatedReconfiguration=true -DreconfInitialDelay=8000 -DreconfInterval=60000 -DDOPmax=16 -DDOPinIsolation=8 -DqueriesPerCategory="{\"2_way_join\":[0,1]}" -DreconfTargets="{\"2_way_join\":[\"statefuljoingid0\",\"statefuljoingid1\"]}" -DfiltersPerQuery="{\"0\":[0,49],\"1\":[0,99]}""
```
