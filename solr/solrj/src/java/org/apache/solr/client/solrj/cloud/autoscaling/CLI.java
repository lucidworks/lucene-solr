package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;

/**
 *
 */
public class CLI {


  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: CLI <zkHost> [<autoscaling.json.file>] [--suggestions] [--diagnostics] [--sortedNodes]");
      System.exit(-1);
    }
    boolean withSortedNodes = false;
    boolean withSuggestions = false;
    boolean withDiagnostics = false;
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
    System.err.println("- Calculating suggestions...");
    long start = TimeSource.NANO_TIME.getTimeNs();
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(config, clientCloudManager);
    long end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
    System.err.println("- Calculating diagnostics...");
    start = TimeSource.NANO_TIME.getTimeNs();
    MapWriter mw = PolicyHelper.getDiagnostics(config.getPolicy(), clientCloudManager);
    Map<String, Object> diagnostics = new LinkedHashMap<>();
    mw.toMap(diagnostics);
    end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
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
