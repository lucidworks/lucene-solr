package org.apache.solr.cloud.autoscaling;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.ZkDistributedQueueFactory;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.TimeSource;

/**
 *
 */
public class CLI {


  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: CLI <zkHost> <autoscaling.json.file>");
      System.exit(-1);
    }
    System.err.println("- Creating CloudSolrClient to zkHost " + args[0]);
    CloudSolrClient client = new CloudSolrClient.Builder(
        Collections.singletonList(args[0]), Optional.empty()).build();
    ZkDistributedQueueFactory queue = new ZkDistributedQueueFactory(client.getZkStateReader().getZkClient());
    SolrClientCloudManager clientCloudManager = new SolrClientCloudManager(queue, client);
    System.err.println("- Reading autoscaling config from " + args[1]);
    AutoScalingConfig config = new AutoScalingConfig(IOUtils.toByteArray(new FileInputStream(args[1])));
    System.err.println("- Calculating suggestions...");
    long start = TimeSource.NANO_TIME.getTimeNs();
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(config, clientCloudManager);
    long end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
    System.err.println("- Getting diagnostics...");
    start = TimeSource.NANO_TIME.getTimeNs();
    MapWriter mw = PolicyHelper.getDiagnostics(config.getPolicy(), clientCloudManager);
    end = TimeSource.NANO_TIME.getTimeNs();
    System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
    System.out.println("\n============= SUGGESTIONS ===========");
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      System.out.println("* " + suggestion.toString());
    }
    System.out.println("\n============= DIAGNOSTICS ===========");
    mw._forEachEntry((k, v) -> {
      System.out.println("* " + k + "\t" + v);
    });
  }
}
