package code.test;
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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Part of stand-alone test for externally beating up changing docValues while indexing
 */
public class IndexerThread implements Runnable {

  final CloudSolrClient client;

  IndexerThread(CloudSolrClient client) {
    this.client = client;
  }

  static String[] facets = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"};
//  static int BATCH_SIZE = 100;

  Random rand = new Random();


  @Override
  public void run() {
    try {
      System.out.println("Indexing batch");
      addDocs(); // get the first set of docs in place, presumably without docValues
      if (AddDvStress.stopRun.get()) {
        return;
      }
//      System.out.println("Starting second cycle");
//      addDocs();
      System.out.println("Indexing done");
    } catch (SolrServerException | IOException | InterruptedException e) {
      e.printStackTrace();
      AddDvStress.stopRun.set(true);
      return;
    }
  }

  boolean doCommit(boolean waitFlush, boolean waitSearcher) throws InterruptedException {
    for (int idx = 0; idx < 10; ++idx) {
      try {
        client.commit(waitFlush, waitSearcher);
        return true;
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }
    AddDvStress.stopRun.set(true);
    return false;
  }

  void addDocs() throws IOException, SolrServerException, InterruptedException {
    List<SolrInputDocument> docs = new ArrayList<>();
    int finalCount = 0;
    for (int idx = 0; finalCount < AddDvStress.docsPerPass.get() && AddDvStress.stopRun.get() == false; idx++) {
      if (AddDvStress.configsChanged.get()) {
        finalCount++;
      }
      SolrInputDocument doc = new SolrInputDocument();
//      doc.addField("id", rand.nextInt(AddDvStress.docsPerPass.get()));
      doc.addField("id", AddDvStress.globalId.getAndIncrement());
      doc.addField(AddDvStress.FIELD, facets[rand.nextInt(facets.length)]);
      docs.add(doc);
      if ((idx % AddDvStress.batchSize.get()) == 0) {
        client.add(docs);
        AddDvStress.docCount.set(AddDvStress.docCount.get() + docs.size()); // No not truly atomic but this is only one thread!
        if (doCommit(false, false) == false) {
          return;
        }
        docs.clear();
//        while (AddDvStress.stopRun.get() == false && AddDvStress.pauseIndexing.get()) {
//          AddDvStress.indexingPaused.set(true);
//          Thread.sleep(100);
//        }
//        AddDvStress.indexingPaused.set(false);
      }
    }
    if (docs.size() > 0) {
      doCommit(true, true);
      AddDvStress.docCount.set(AddDvStress.docCount.get() + docs.size()); // No not truly atomic but this is only one thread!
    }
  }
}
