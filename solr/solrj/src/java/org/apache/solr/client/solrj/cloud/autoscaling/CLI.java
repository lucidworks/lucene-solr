package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;

/**
 *
 */
public class CLI {


  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: CLI <zkHost> [<autoscaling.json.file>] ([--full] | [--stats] [--suggestions] [--diagnostics] [--sortedNodes])");
      System.err.println("\t--full\treturn all available information");
      System.err.println("\t--stats\tsummarized collection & node stats");
      System.err.println("\t--suggestions\treturn suggestions");
      System.err.println("\t--diagnostics\treturn diagnostics (without the 'sortedNodes' section");
      System.err.println("\t--sortedNodes\treturn diagnostics with the 'sortedNodes' section");
      System.exit(-1);
    }
    boolean withSortedNodes = false;
    boolean withSuggestions = false;
    boolean withDiagnostics = false;
    boolean withStats = false;
    System.err.println("- Creating CloudSolrClient to " + args[0]);
    CloudSolrClient client = new CloudSolrClient.Builder(
        Collections.singletonList(args[0]), Optional.empty()).build();
    DistributedQueueFactory dummmyFactory = new DistributedQueueFactory() {
      @Override
      public DistributedQueue makeQueue(String path) throws IOException {
        throw new UnsupportedOperationException("makeQueue");
      }

      @Override
      public void removeQueue(String path) throws IOException {
        throw new UnsupportedOperationException("removeQueue");
      }
    };
    SolrClientCloudManager clientCloudManager = new SolrClientCloudManager(dummmyFactory, client);
    AutoScalingConfig config = null;
    if (args.length > 1) {
      for (int i = 1; i < args.length; i++) {
        String arg = args[i];
        if (arg.startsWith("--")) { // option
          if (arg.equals("--suggestions")) {
            withSuggestions = true;
          } else if (arg.equals("--diagnostics")) {
            withDiagnostics = true;
          } else if (arg.equals("--sortedNodes")) {
            withSortedNodes = true;
          } else if (arg.equals("--stats")) {
            withStats = true;
          } else if (arg.equals("--full")) {
            withDiagnostics = true;
            withSortedNodes = true;
            withSuggestions = true;
            withStats = true;
          } else {
            System.err.println("Unrecognized option: " + arg);
            System.exit(-1);
          }
        } else {
          System.err.println("- Reading autoscaling config from " + arg);
          config = new AutoScalingConfig(IOUtils.toByteArray(new FileInputStream(arg)));
        }
      }
    }
    if (config == null) {
      System.err.println("- Reading autoscaling config from the cluster");
      config = clientCloudManager.getDistribStateManager().getAutoScalingConfig();
    }
    if (withSortedNodes) {
      withDiagnostics = true;
    }
    ClusterState clusterState = clientCloudManager.getClusterStateProvider().getClusterState();
    Map<String, Map<String, Number>> collStats = new TreeMap<>();
    clusterState.forEachCollection(coll -> {
      Map<String, Number> perColl = collStats.computeIfAbsent(coll.getName(), n -> new LinkedHashMap<>());
      AtomicInteger numCores = new AtomicInteger();
      HashMap<String, Map<String, AtomicInteger>> nodes = new HashMap<>();
      coll.getSlices().forEach(s -> {
        numCores.addAndGet(s.getReplicas().size());
        s.getReplicas().forEach(r -> {
          nodes.computeIfAbsent(r.getNodeName(), n -> new HashMap<>())
            .computeIfAbsent(s.getName(), slice -> new AtomicInteger()).incrementAndGet();
        });
      });
      int maxCoresPerNode = 0;
      int minCoresPerNode = 0;
      int maxActualShardsPerNode = 0;
      int minActualShardsPerNode = 0;
      int maxShardReplicasPerNode = 0;
      int minShardReplicasPerNode = 0;
      if (!nodes.isEmpty()) {
        minCoresPerNode = Integer.MAX_VALUE;
        minActualShardsPerNode = Integer.MAX_VALUE;
        minShardReplicasPerNode = Integer.MAX_VALUE;
        for (Map<String, AtomicInteger> counts : nodes.values()) {
          int total = counts.values().stream().mapToInt(c -> c.get()).sum();
          for (AtomicInteger count : counts.values()) {
            if (count.get() > maxShardReplicasPerNode) {
              maxShardReplicasPerNode = count.get();
            }
            if (count.get() < minShardReplicasPerNode) {
              minShardReplicasPerNode = count.get();
            }
          }
          if (total > maxCoresPerNode) {
            maxCoresPerNode = total;
          }
          if (total < minCoresPerNode) {
            minCoresPerNode = total;
          }
          if (counts.size() > maxActualShardsPerNode) {
            maxActualShardsPerNode = counts.size();
          }
          if (counts.size() < minActualShardsPerNode) {
            minActualShardsPerNode = counts.size();
          }
        }
      }
      perColl.put("activeShards", coll.getActiveSlices().size());
      perColl.put("inactiveShards", coll.getSlices().size() - coll.getActiveSlices().size());
      perColl.put("rf", coll.getReplicationFactor());
      perColl.put("maxShardsPerNode", coll.getMaxShardsPerNode());
      perColl.put("maxActualShardsPerNode", maxActualShardsPerNode);
      perColl.put("minActualShardsPerNode", minActualShardsPerNode);
      perColl.put("maxShardReplicasPerNode", maxShardReplicasPerNode);
      perColl.put("minShardReplicasPerNode", minShardReplicasPerNode);
      perColl.put("numCores", numCores.get());
      perColl.put("numNodes", nodes.size());
      perColl.put("maxCoresPerNode", maxCoresPerNode);
      perColl.put("minCoresPerNode", minCoresPerNode);
    });
    System.err.println("- Calculating suggestions...");
    long start = TimeSource.NANO_TIME.getTimeNs();
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(config, clientCloudManager);
    long end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
    System.err.println("- Calculating diagnostics...");
    start = TimeSource.NANO_TIME.getTimeNs();
    Policy.Session session = config.getPolicy().createSession(clientCloudManager);
    MapWriter mw = PolicyHelper.getDiagnostics(session);
    Map<String, Object> diagnostics = new LinkedHashMap<>();
    mw.toMap(diagnostics);
    Map<String, Map<String, Object>> nodeStats = new TreeMap<>();
    for (Row row : session.getSortedNodes()) {
      Map<String, Object> nodeStat = nodeStats.computeIfAbsent(row.node, n -> new LinkedHashMap<>());
      nodeStat.put("isLive", row.isLive);
      nodeStat.put("freedisk", row.getVal("freedisk"));
      nodeStat.put("totaldisk", row.getVal("totaldisk"));
      nodeStat.put("cores", row.getVal("cores"));
      Map<String, Map<String, Map<String, Object>>> collReplicas = new TreeMap<>();
      row.forEachReplica(ri -> {
        Map<String, Object> perReplica = collReplicas.computeIfAbsent(ri.getCollection(), c -> new TreeMap<>())
            .computeIfAbsent(ri.getCore().substring(ri.getCollection().length() + 1), core -> new LinkedHashMap<>());
        perReplica.put("INDEX.sizeInGB", ri.getVariable("INDEX.sizeInGB"));
        perReplica.put("coreNode", ri.getName());
        if (ri.getBool("leader", false)) {
          perReplica.put("leader", true);
          Double totalSize = (Double)collStats.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
              .computeIfAbsent("avgShardSize", size -> new Double(0));
          Number riSize = (Number)ri.getVariable("INDEX.sizeInGB");
          if (riSize != null) {
            totalSize += riSize.doubleValue();
            collStats.get(ri.getCollection()).put("avgShardSize", totalSize);
            Double max = (Double)collStats.get(ri.getCollection()).get("maxShardSize");
            if (max == null) max = 0.0;
            if (riSize.doubleValue() > max) {
              collStats.get(ri.getCollection()).put("maxShardSize", riSize.doubleValue());
            }
            Double min = (Double)collStats.get(ri.getCollection()).get("minShardSize");
            if (min == null) min = Double.MAX_VALUE;
            if (riSize.doubleValue() < min) {
              collStats.get(ri.getCollection()).put("minShardSize", riSize.doubleValue());
            }
          }
        }
        nodeStat.put("replicas", collReplicas);
      });
    }

    // calculate average per shard
    for (Map<String, Number> perColl : collStats.values()) {
      Double avg = (Double)perColl.get("avgShardSize");
      if (avg != null) {
        avg = avg / ((Number)perColl.get("activeShards")).doubleValue();
        perColl.put("avgShardSize", avg);
      }
    }

    end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
    if (withStats) {
      System.out.println("\n============= NODE STATISTICS ===========");
      System.out.println(Utils.toJSONString(nodeStats));
      System.out.println("\n============= COLLECTION STATISTICS ===========");
      System.out.println(Utils.toJSONString(collStats));
    }
    if (withSuggestions) {
      System.out.println("\n============= SUGGESTIONS ===========");
      System.out.println(Utils.toJSONString(suggestions));
    }
    if (!withSortedNodes) {
      diagnostics.remove("sortedNodes");
    }
    if (withDiagnostics) {
      System.out.println("\n============= DIAGNOSTICS ===========");
      System.out.println(Utils.toJSONString(diagnostics));
    }
    System.exit(0);
  }
}
