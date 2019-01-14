package org.apache.solr.index;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Time;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

//TODO EOE put this back @LuceneTestCase.Slow
public class AddDvWhileIndexingTest extends AbstractFullDistribZkTestBase {

  public static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static String COLLECTION_NAME = "AddDvWhileIndexing";

  final static AtomicBoolean stopRun = new AtomicBoolean(false);
  final static int QUERY_SLEEP_INTERVAL_MS = 500;

  final static int BATCH_SIZE = 10; // we want lots of merging going on.
  final static int NUM_DOCS = 5_000;

  final static String TEST_FIELD = "add_dv_test_field";

  public AddDvWhileIndexingTest() {
    super();
    sliceCount = 1;
    schemaString = "schema-id-and-version-fields-only.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    // TOCO worry about this later.
    return "solrconfig-adddvmergepolicy.xml";
  }

  @Before
  public void before() throws Exception {
    // First delete the collection if present
    deleteCollection();

    // Add the collection
    createCollection();

    cloudClient.setDefaultCollection(COLLECTION_NAME);
  }

  @Test
  public void testAddDvWhileIndexing() throws Exception {
    // add the field w/o docValues
    updateSchema(false);

    // Start the indexing thread
    Thread indexerThread = new Thread(new IndexerThread(cloudClient, random().nextLong()));
    indexerThread.start();

    // Start the query thread
    Thread queryThread = new Thread(new QueryThread(cloudClient));
    queryThread.start();

    indexerThread.join();
    queryThread.join();
    // We've indexed a bunch of docs, now update the schema and run the threads over again.

    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(cloudClient);

    Map<String, Object> field = getSchemaField(TEST_FIELD, schemaResponse);
    assertNotNull("Should be able to get field back!", field);

    log.info("******* EOE test field should have docValues false, value: {}", field.get("docValues"));

    assertFalse("Doc values for test field should be false at this point.", (Boolean)field.get("docValues"));

    try {
      // Now change the schema field to have docValues=true
      updateSchema(true);
    } catch (IOException | SolrServerException e) {
      AddDvWhileIndexingTest.stopRun.set(true);
      AddDvWhileIndexingTest.log.error("***** EOE About to fail in ChangerThread due to exception {}", e);
      fail("Was unable to update the configuration " + e.getMessage());
    }

    schemaRequest = new SchemaRequest();
    schemaResponse = schemaRequest.process(cloudClient);

    field = getSchemaField(TEST_FIELD, schemaResponse);
    assertNotNull("Should be able to get field back!", field);

    log.info("******* EOE test field should have docValues true, value: {}", field.get("docValues"));

    assertTrue("Doc values for test field should be true at this point.", (Boolean)field.get("docValues"));

    // Start the indexing and query threads again to see if background merging is doing the right thing
    stopRun.set(false);

    queryThread = new Thread(new QueryThread(cloudClient));
    queryThread.start();

    indexerThread = new Thread(new IndexerThread(cloudClient, random().nextLong()));
    indexerThread.start();

    indexerThread.join();
    queryThread.join();

  }

  private Map<String, Object> getSchemaField(String name, SchemaResponse schemaResponse) {
    List<Map<String, Object>> fields = schemaResponse.getSchemaRepresentation().getFields();
    for (Map<String, Object> field : fields) {
      if (name.equals(field.get("name"))) {
        return field;
      }
    }
    return null;
  }

  void updateSchema(boolean dv) throws Exception {
    log.info("***** EOE about to update {} with docValues of {}", TEST_FIELD, dv);

    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", TEST_FIELD);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("required", false);
    fieldAttributes.put("docValues", dv);

    // I want this to be able to run with tests.iters, so the field may or may not have the value at the start.
    // So first try to update the existing field. If that fails, assume it needs to be added. Either way,
    // there should be success at the end.

    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(fieldAttributes);
    SchemaResponse.UpdateResponse updateResponse = replaceFieldRequest.process(cloudClient);

    if (updateResponse.getStatus() != 0 || updateResponse.getResponse().get("errors") != null) {
      SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(fieldAttributes);
      updateResponse = addFieldRequest.process(cloudClient);
    }

    //TODO EOE
    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    reloadCollection(cloudClient);

    //TODO EOE
    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    waitForRecoveriesToFinish(COLLECTION_NAME, false);

    log.info("***** EOE Updated schema successfully, also waited for recoveries to complete");
    assertTrue("Could not update the schema!" + updateResponse.getResponse().get("errors")
        , updateResponse.getStatus() == 0 && updateResponse.getResponse().get("errors") == null);

  }

  static void reloadCollection(CloudSolrClient client) throws IOException, SolrServerException {
    final CollectionAdminRequest.Reload reloadCollectionRequest = new CollectionAdminRequest.Reload()
        .setCollectionName(AddDvWhileIndexingTest.COLLECTION_NAME);

    CollectionAdminResponse resp = reloadCollectionRequest.process(client);

    assertTrue("Could not reload collection!", resp.getStatus() == 0 && resp.isSuccess());
  }

  private void deleteCollection() throws IOException, SolrServerException {

    DocCollection dc = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(COLLECTION_NAME);
    if (dc == null) return;

    final CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete()
        .setCollectionName(COLLECTION_NAME);

    final CollectionAdminResponse response = deleteCollectionRequest.process(cloudClient);
    assertEquals("Failed to delete collection: ", 0, response.getStatus());
  }

  private void createCollection() throws Exception {
    final CollectionAdminResponse response = new CollectionAdminRequest.Create()
        .setCollectionName(COLLECTION_NAME)
        .setNumShards(1)
        .setReplicationFactor(1)
        .setConfigName("conf1").process(cloudClient);
    assertTrue("Failed to create collection: ", response.isSuccess());
    waitForRecoveriesToFinish(COLLECTION_NAME, false);
    cloudClient.setDefaultCollection(COLLECTION_NAME);
    log.info("**** EOE Created collction: {}", COLLECTION_NAME);
  }
}

class QueryThread implements Runnable {
  final CloudSolrClient client;

  QueryThread(final CloudSolrClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    AddDvWhileIndexingTest.log.info("***** EOE Starting query thread");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.field", AddDvWhileIndexingTest.TEST_FIELD);
    params.add("facet.limit", "-1");

    int facetTotal = 0;

    while (AddDvWhileIndexingTest.stopRun.get() == false) {
      try {
        TimeUnit.MILLISECONDS.sleep(AddDvWhileIndexingTest.QUERY_SLEEP_INTERVAL_MS);
        QueryResponse resp = client.query(params);
        FacetField ff = resp.getFacetField(AddDvWhileIndexingTest.TEST_FIELD);
        facetTotal = 0;

        for (FacetField.Count count : ff.getValues()) {
          facetTotal += count.getCount();
        }
        assertEquals("There should be exactly as many facets as there are documents!"
            , resp.getResults().getNumFound(), facetTotal);
      } catch (IOException | SolrServerException | InterruptedException e) {
        AddDvWhileIndexingTest.stopRun.set(true);
        AddDvWhileIndexingTest.log.error("***** EOE Query thread caught exception {}", e);
        fail("Caught exception in query thread:");
      }
    }
    assertTrue("Guard against real silliness, we should have counted some facets last time through", facetTotal > 0);
    AddDvWhileIndexingTest.log.info("***** EOE Exiting query thread");
  }
}

class IndexerThread implements Runnable {

  final CloudSolrClient client;
  final Random rand;

  IndexerThread(final CloudSolrClient client, long seed) {
    this.client = client;
    rand = new Random(seed);
  }

  static String[] facets = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"};

  @Override
  public void run() {
    try {
      AddDvWhileIndexingTest.log.info("***** EOE Launching indexer thread");
      addDocs();
    } catch (SolrServerException | IOException | InterruptedException e) {
      AddDvWhileIndexingTest.log.error("***** EOE Caught unexpected exception in Indexer thread {}", e);
      AddDvWhileIndexingTest.stopRun.set(true);
      return;
    }
    AddDvWhileIndexingTest.log.info("***** EOE Leaving indexer thread, indexed {} documents", AddDvWhileIndexingTest.NUM_DOCS);
    AddDvWhileIndexingTest.stopRun.set(true);
  }

  void addDocs() throws IOException, SolrServerException, InterruptedException {
    List<SolrInputDocument> docs = new ArrayList<>();
    try {
      for (int idx = 0; idx < AddDvWhileIndexingTest.NUM_DOCS && AddDvWhileIndexingTest.stopRun.get() == false; idx++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", rand.nextInt(AddDvWhileIndexingTest.NUM_DOCS));
        doc.addField(AddDvWhileIndexingTest.TEST_FIELD, facets[rand.nextInt(facets.length)]);
        docs.add(doc);
        if (idx > 0 && (idx % AddDvWhileIndexingTest.BATCH_SIZE) == 0) {
          UpdateResponse resp = client.add(docs);
          assertEquals("Update failed: ", 0, resp.getStatus());
          resp = client.commit(true, true);
          assertEquals("Commit failed: ", 0, resp.getStatus());
        }
        // Just let the log know how many docs we've indexed. Critically, this message should come out both before and
        // after the config has chanbged.
        if ((idx % 1_000) == 0) {
          AddDvWhileIndexingTest.log.info("***** EOE Indexed {} documents so far", idx);
        }

        docs.clear();
      }
      if (docs.size() > 0) {
        client.commit(true, true);
      }
    } catch (Exception e) {
      AddDvWhileIndexingTest.log.error("******EOE Caught unexpected exception {}", e);
      AddDvWhileIndexingTest.stopRun.set(true); // We're done!
      fail("Caught unexpected exception");
    }
    AddDvWhileIndexingTest.stopRun.set(true); // We're done!
  }
}


