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
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.apache.solr.common.cloud.ZkCredentialsProvider;

import static org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider.*;
import static org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider.*;

import org.apache.zookeeper.KeeperException.NoAuthException;

import org.junit.BeforeClass;

/**
 * Test the process of retirig old Zk credentials by restarting solr nodes configured with old + new, then using updateacls, 
 * then restarting again only with new crednetails and running updateacls again.
 */
public class RestartCloudWithNewZkCredentialsTest extends SolrCloudTestCase {
  
  private static final Properties credentials = new Properties();
  private static File credentialsFile = null;
  private static void writeCredProps() {
    try (Writer out = new OutputStreamWriter(new FileOutputStream(credentialsFile), StandardCharsets.UTF_8)) {
      credentials.store(out, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** 
   * deterministic password for any given username
   * we're not testing passwords - this make it easier for test "macros" to only worry about passing around usernames
   */
  private static String password(final String user) {
    return user + "_123Password";
  }
  
  @BeforeClass
  public static void setupCluster() throws Exception {

    // one time zk cred + acls property setup to point at file (that we'll replace over and over)....
    credentialsFile = createTempFile(DEFAULT_DIGEST_FILE_VM_PARAM_NAME, "properties").toFile();
    System.setProperty(DEFAULT_DIGEST_FILE_VM_PARAM_NAME,
                       credentialsFile.getAbsolutePath());
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
                       VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
                       VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    // MiniSolrCloudCluster (and CloudSolrClient) have no mechanism for replacing crecdentials (or SolrZkClient) on the fly
    // so we have to start the "cluster" with an "extra" set of test_artifact credentials that we will *always* give at least read access to
    credentials.setProperty(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME+".test_artifact", "test_artifact");
    credentials.setProperty(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME+".test_artifact", password("test_artifact"));
    writeCredProps();
        
    // create and configure cluster to boostrap the MiniSolrCloudClient
    configureCluster(1) // we can't start with 0
      .addConfig("conf", configset("cloud-dynamic"))
      .configure();
    // shutdown our (only 1) nodes until we fix the credentials
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      cluster.stopJettySolrRunner(jetty);
      cluster.waitForJettyToStop(jetty);
    }

    // ADD zk credentials & acls for "admin=alice; readonly=roger" 
    credentials.setProperty(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "alice");
    credentials.setProperty(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, password("alice"));
    credentials.setProperty(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "roger");
    credentials.setProperty(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, password("roger"));
    writeCredProps();
    updateacls();
    // ...and now use those new credentials and updatedacls to demote our test_artifact user to read only...
    // (from here on MiniSolrCloudCluster should only need read access to zk)
    credentials.remove(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME+".test_artifact");
    credentials.remove(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME+".test_artifact");
    credentials.setProperty(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME+".test_artifact", "test_artifact");
    credentials.setProperty(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME+".test_artifact", password("test_artifact"));
    writeCredProps();
    updateacls();

    // confirm our test_artifact user can read things MiniSolrCloudCluster might care about...
    assertZkAccess("test_artifact",
                   Arrays.asList("/aliases.json",
                                 "/live_nodes"),
                   Arrays.asList("/security.json"));

    
    // now start up 2 "real" nodes using the "real" credentials * acls
    for (int i =0; i < 2; i++) {
      cluster.waitForNode(cluster.startJettySolrRunner(), 60);
    }

    // sanity check initial collection can be created ok
    createCollectionAndAddOneDoc("xxx");
    
    // confirm that alice can read any zk data (including security.json)
    assertZkAccess("alice",
                   Arrays.asList("/security.json",
                                 "/aliases.json",
                                 "/collections/xxx/state.json"), 
                   Collections.emptyList());
    // confirm that roger can read most zk data (but not security)
    assertZkAccess("roger",
                   Arrays.asList("/aliases.json",
                                 "/collections/xxx/state.json"),
                   Arrays.asList("/security.json"));
    
    // confirm our test_artifact user can read things MiniSolrCloudCluster might care about...
    assertZkAccess("test_artifact",
                   Arrays.asList("/aliases.json",
                                 "/live_nodes",
                                 "/collections/xxx/state.json"), 
                   Arrays.asList("/security.json"));
  }

  public void testCredentialSwapProcess() throws Exception {
    
    // change zk credentials & acls to ADD "admin=adam; readonly=ruth" (still also have "admin=alice; readonly=roger")
    credentials.setProperty(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME+".Q2", "adam");
    credentials.setProperty(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME+".Q2", password("adam"));
    credentials.setProperty(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME+".Q2", "ruth");
    credentials.setProperty(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME+".Q2", password("ruth"));
    writeCredProps();

    // restart all nodes one at a time to pick up new Zk credentials & ACLs...
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      cluster.stopJettySolrRunner(jetty);
      cluster.waitForJettyToStop(jetty);
      cluster.startJettySolrRunner(jetty);
      cluster.waitForNode(jetty, 60);
      cluster.waitForActiveCollection("xxx", 1, 2);
    }

    // updateacls (fully adding adam & ruth to existing paths)
    updateacls();
    
    // confirm solr can add a new collection with updated credentials / acls
    createCollectionAndAddOneDoc("yyy");

    // confirm that BOTH alice & adam can read any zk data (including security.json)
    for (String user : Arrays.asList("alice", "adam")) {
      assertZkAccess(user,
                     Arrays.asList("/security.json",
                                   "/aliases.json",
                                   "/collections/xxx/state.json", 
                                   "/collections/yyy/state.json"), 
                     Collections.emptyList());
    }
    // confirm that BOTH roger & ruth can read most zk data (but not security)
    for (String user : Arrays.asList("roger", "ruth")) {
      assertZkAccess(user,
                     Arrays.asList("/aliases.json",
                                   "/collections/xxx/state.json",
                                   "/collections/yyy/state.json"),
                     Arrays.asList("/security.json"));
    }

    // change zk credentials & acls to REMOVE "admin=alice; readonly=roger" (keeping "admin=adam; readonly=ruth")
    credentials.remove(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME);
    credentials.remove(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
    credentials.remove(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
    credentials.remove(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
    writeCredProps();
      
    // restart all nodes one at a time to pick up new Zk credentials & ACLs...
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      cluster.stopJettySolrRunner(jetty);
      cluster.waitForJettyToStop(jetty);
      cluster.startJettySolrRunner(jetty);
      cluster.waitForNode(jetty, 60);
      cluster.waitForActiveCollection("xxx", 1, 2);
      cluster.waitForActiveCollection("yyy", 1, 2);
    }
    
    // updateacls (fully removing alice & roger)
    updateacls();
    
    // confirm solr can add a new collection with updated credentials / acls
    createCollectionAndAddOneDoc("zzz");

    // confirm that "adam" can read zk data for both collections (including security)
    assertZkAccess("adam",
                   Arrays.asList("/security.json",
                                 "/aliases.json",
                                 "/collections/xxx/state.json", 
                                 "/collections/yyy/state.json", 
                                 "/collections/zzz/state.json"), 
                   Collections.emptyList());
    // confirm that "ruth" can read zk data for both collections (but not security)
    assertZkAccess("ruth",
                   Arrays.asList("/aliases.json",
                                 "/collections/xxx/state.json",
                                 "/collections/yyy/state.json",
                                 "/collections/zzz/state.json"),
                   Arrays.asList("/security.json"));
    
    // confirm that BOTH alice & roger can NOT read any zk data
    for (String user : Arrays.asList("alice", "roger")) {
      assertZkAccess(user,
                     Collections.emptyList(),
                     Arrays.asList("/security.json",
                                   "/aliases.json",
                                   "/collections/xxx/state.json",
                                   "/collections/yyy/state.json",
                                   "/collections/zzz/state.json"));
    }
  }

  /** Helper macro for sanity checing the cluster can create a (usable) collection */
  protected static void createCollectionAndAddOneDoc(final String collection) throws Exception {
    // create an empty collection
    CollectionAdminRequest.createCollection(collection, "conf", 1, 2)
      .setMaxShardsPerNode(1)
      .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collection, 1, 2);
    
    // add a document
    new UpdateRequest()
      .add(sdoc("id", "1", "my_collection_s", collection))
      .commit(cluster.getSolrClient(), collection);
    
    // sanity check
    assertEquals(1, cluster.getSolrClient().query(collection, new SolrQuery("q", "my_collection_s:" + collection, "rows", "5")).getResults().size());
  }

  /**
   * Uses a SolrZkClient that picks up the (current) default credentials + ACLs to update all acls in the cluster
   */
  protected static void updateacls() throws Exception {
    try (SolrZkClient solrZk = new SolrZkClient(cluster.getZkClient().getZkServerAddress(), cluster.getZkClient().getZkClientTimeout())) {
      solrZk.updateACLs("/");
    }
  }

  /**
   * Asserts that a SolrZkClient using the specified user's credentails can "getData" on the allowed paths, but gets a NoAuthException
   * on all denied paths
   */
  protected static void assertZkAccess(final String user,
                                       final List<String> allowedPaths,
                                       final List<String> deniedPaths) throws Exception {
    try (SolrZkClient solrZk = new SolrZkClient(cluster.getZkClient().getZkServerAddress(), cluster.getZkClient().getZkClientTimeout()) {
      @Override
      protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
        return new SingleUserZkCreds(user);
      }
    }) {
      for (String allowedPath : allowedPaths) {
        try {
          solrZk.getData(allowedPath, null, null, false);
        } catch (NoAuthException e) {
          assertNull("Unexpected auth exception for "+user+" on path:" + allowedPath,
                     e);
        }
      }
      for (String deniedPath : deniedPaths) {
        NoAuthException e = expectThrows(NoAuthException.class, () -> solrZk.getData(deniedPath, null, null, false));
        assertEquals(deniedPath, e.getPath());
      }
    }
  }

  private static final class SingleUserZkCreds implements ZkCredentialsProvider {
    private final String user;
    public SingleUserZkCreds(final String user) {
      this.user = user;
    }
    public Collection<ZkCredentials> getCredentials() {
      return Arrays.asList(new ZkCredentials("digest", (user + ":" + password(user)).getBytes(StandardCharsets.UTF_8)));
    }
  }
}

