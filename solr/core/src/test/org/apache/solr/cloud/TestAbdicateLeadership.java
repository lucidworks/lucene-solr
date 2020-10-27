/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAbdicateLeadership extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf1", configset("cloud-minimal"))
        .configure();

  }

  public void testMultipleCollectionsWithStressIndexing() throws Exception {
    List<String> collections = new ArrayList<>();
    // by Dirichlet's theorem there will always a node that host more than 1 leader
    for (int i = 0; i < cluster.getJettySolrRunners().size() + 1; i++) {
      String collection = "collection"+i+"_stress";
      CollectionAdminRequest.createCollection(collection, "conf1", 1, 3)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collection, 1, 3);
      collections.add(collection);
    }
    AtomicBoolean closed = new AtomicBoolean(false);
    Thread[] threads = new Thread[30];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        while (!closed.get()) {
          String collection = collections.get(random().nextInt(collections.size()));
          assert collection != null;
          try {
            cluster.getSolrClient().add(collection, new SolrInputDocument("id", String.valueOf(random().nextInt())));
            Thread.sleep(20);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            //ignore
          }
        }
      });
      threads[i].start();
    }
    //warm up indexing
    Thread.sleep(4000);

    JettySolrRunner randomNode = cluster.getJettySolrRunners()
        .get(random().nextInt(cluster.getJettySolrRunners().size()));
    try (SolrClient solrClient = getHttpSolrClient(randomNode.getBaseUrl().toString())){
      CoreAdminResponse response = CoreAdminRequest.abdicateLeadership(solrClient);
    }
    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
    timeOut.waitFor("Timeout waiting for a new leader", () -> {
      ClusterState state = cluster.getSolrClient().getZkStateReader().getClusterState();
      for (String collection : collections) {
        Replica leader = state.getCollection(collection).getSlice("shard1").getLeader();
        if (leader == null) {
          return false;
        }
        if (randomNode.getNodeName().equals(leader.getNodeName())) {
          return false;
        }
      }
      return true;
    });

    closed.set(true);
    for (Thread thread: threads) {
      thread.join();
    }
    for (String collection: collections) {
      cluster.waitForActiveCollection(collection, 1, 3);
    }
  }

  @After
  public void deleteCollections() throws IOException, SolrServerException {
    List<String> collections = CollectionAdminRequest.listCollections(cluster.getSolrClient());
    for (String collection : collections) {
      CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    }
  }

  @Test
  public void testSimple() throws Exception {
    String collection = "collection1_testsimple";
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collection, "conf1", 1, 3) .process(cluster.getSolrClient());
    assertEquals("Admin request failed; ", 0, resp.getStatus());
    cluster.waitForActiveCollection(collection, 1, 3);
    DocCollection dc = cluster.getSolrClient().getZkStateReader()
        .getClusterState().getCollection(collection);
    Replica leaderReplica = dc.getSlice("shard1").getLeader();
    JettySolrRunner runner = cluster.getReplicaJetty(leaderReplica);
    AtomicBoolean closed = new AtomicBoolean(false);
    Thread thread = new Thread(() -> {
      int i = 0;
      while (!closed.get()) {
        try {
          cluster.getSolrClient().add(collection, new SolrInputDocument("id", String.valueOf(i++)));
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Exception e) { e.printStackTrace(); }
      }
    });
    thread.start();
    try (SolrClient solrClient = getHttpSolrClient(runner.getBaseUrl().toString())){
      CoreAdminResponse response = CoreAdminRequest.abdicateLeadership(solrClient);
    }
    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
    timeOut.waitFor("Timeout waiting for a new leader", () -> {
      ClusterState state = cluster.getSolrClient().getZkStateReader().getClusterState();
      Replica newLeader = state.getCollection(collection).getSlice("shard1").getLeader();
      return newLeader != null && !newLeader.getNodeName().equals(runner.getNodeName());
    });
    closed.set(true);
    thread.join();
  }

}
