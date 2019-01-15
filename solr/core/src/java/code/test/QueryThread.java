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
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.*;

/**
 * Part of stand-alone test for externally beating up changing docValues while indexing
 */

public class QueryThread implements Runnable {
  final CloudSolrClient client;

  QueryThread(CloudSolrClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    while (AddDvStress.stopRun.get() == false && AddDvStress.endCycle.get() == false) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("facet", "true");
      params.add("facet.field", AddDvStress.FIELD);
      params.add("facet.limit", "-1");
      int total = 0;
      long numFound = 0;
      try {
        Thread.sleep(AddDvStress.querySleepInterval.get());
        QueryResponse resp = client.query(params);
        FacetField ff = resp.getFacetField(AddDvStress.FIELD);

        for (FacetField.Count count : ff.getValues()) {
          total += count.getCount();
        }
        numFound = resp.getResults().getNumFound();
        if (numFound != total) {
          System.out.println("Found: " + total + " expected: " + numFound);
          AddDvStress.badState.set(true);
          AddDvStress.querySleepInterval.set(1000);
        } else {
          AddDvStress.badState.set(false);
          AddDvStress.querySleepInterval.set(100);
        }
      } catch (IOException | SolrServerException | InterruptedException e) {
        e.printStackTrace();
        AddDvStress.stopRun.set(true);
      }
      if (AddDvStress.endCycle.get()) {
        System.out.println("Final query, found: " + total + " expected: " + numFound + " docCount: " + AddDvStress.docCount.get());
        if (total != numFound) {
          AddDvStress.stopRun.set(true);
        }
      }
    }
  }
}
