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
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class VMParamsZkACLAndCredentialsProvidersTest extends SolrTestCaseJ4 {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Charset DATA_ENCODING = Charset.forName("UTF-8");
  
  protected ZkTestServer zkServer;
  
  protected Path zkDir;

  
  private static File credentialsFile = null;

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return Arrays.<Object[]>asList(new Object[] { false },
                                   new Object[] { true });
  }

  public VMParamsZkACLAndCredentialsProvidersTest(Boolean useFile) throws Exception {
    assert null != useFile;
    credentialsFile = null;
    if (useFile) {
      credentialsFile = createTempFile(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME, "properties").toFile();
    }
  }

  /** Does the correct thing with credential props depending on wether a file is being used 
   *
   * NOTE: this only sets the props you ask it to, it doesn't "clear" any existing system props by prefix
   */
  public static void setCredentialProps(Properties props) throws Exception {
    if (null != credentialsFile) {
      System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME,
                         credentialsFile.getAbsolutePath());
      try (Writer out = new OutputStreamWriter(new FileOutputStream(credentialsFile), StandardCharsets.UTF_8)) {
        props.store(out, null);
      }
    } else {
      for (String prop : props.stringPropertyNames()) {
        // NOTE: don't use 'System.setProperties(props)' ... it completely replaces the set of all sys props
        System.setProperty(prop, props.getProperty(prop));
      }
    }
  }
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }
    createTempDir();
    
    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);
    
    System.setProperty("zkHost", zkServer.getZkAddress());
    
    setSecuritySystemProperties();

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null, null, null);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.create("/protectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/protectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);

    zkClient.create(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.close();
    
    clearSecuritySystemProperties();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    // Currently no credentials on ZK connection, because those same VM-params are used for adding ACLs, and here we want
    // no (or completely open) ACLs added. Therefore hack your way into being authorized for creating anyway
    zkClient.getSolrZooKeeper().addAuthInfo("digest", ("solradmin:solradminPassword")
        .getBytes(StandardCharsets.UTF_8));
    zkClient.create("/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/unprotectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.close();

    if (log.isInfoEnabled()) {
      log.info("####SETUP_END {}", getTestName());
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();
    
    clearSecuritySystemProperties();
    
    super.tearDown();
  }
  
  @Test
  public void testNoCredentials() throws Exception {
    useNoCredentials();
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          false, false, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testWrongCredentials() throws Exception {
    useWrongCredentials();
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          false, false, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testAllAccessCredentials() throws Exception {
    doTestAllAccessCredentials("solradmin");
  }
  @Test
  public void testAllAccessCredentialsAlt() throws Exception {
    doTestAllAccessCredentials("superUser2");
  }
  private void doTestAllAccessCredentials(final String u) throws Exception {
    useAllAccessCredentials(u);

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          true, true, true, true, true,
          true, true, true, true, true);
    } finally {
      zkClient.close();
    }
  }
  
  @Test
  public void testReadonlyCredentials() throws Exception {
    useReadonlyCredentials();

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          true, true, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testRepairACL() throws Exception {
    clearSecuritySystemProperties();
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      // Currently no credentials on ZK connection, because those same VM-params are used for adding ACLs, and here we want
      // no (or completely open) ACLs added. Therefore hack your way into being authorized for creating anyway
      zkClient.getSolrZooKeeper().addAuthInfo("digest", ("solradmin:solradminPassword")
          .getBytes(StandardCharsets.UTF_8));

      zkClient.create("/security.json", "{}".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, false);
      assertEquals(OPEN_ACL_UNSAFE, zkClient.getACL("/security.json", null, false));
    }

    setSecuritySystemProperties();
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      ZkController.createClusterZkNodes(zkClient);
      assertNotEquals(OPEN_ACL_UNSAFE, zkClient.getACL("/security.json", null, false));
    }

    useReadonlyCredentials();
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      NoAuthException e = expectThrows(NoAuthException.class, () -> zkClient.getData("/security.json", null, null, false));
      assertEquals("/security.json", e.getPath());
    }
  }
    
  @Test
  public void testUpdateACL() throws Exception {

    useAllAccessCredentials("superUser2", "superUser3");
    
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      // 'superUser2' should be able to lock out 'solradmin'...
      zkClient.updateACLs("/");
    }
    
    // this (locked out) 'solradmin' client shouldn't be able to read anything anymore (even the path that was originally unprotected) ...
    useAllAccessCredentials("solradmin", "solradminPassword");
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      for (String path : Arrays.asList(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, "/protectedCreateNode", "/unprotectedCreateNode")) {
        expectThrows(NoAuthException.class, () -> {
            zkClient.getData(path, null, null, false);
          });
      }
    }

    // read only user client should still have access to basic "protected" paths (but still not security)
    useReadonlyCredentials();
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      for (String path : Arrays.asList("/protectedCreateNode", "/unprotectedCreateNode")) {
        assertNotNull(zkClient.getData(path, null, null, false));
      }
      expectThrows(NoAuthException.class, () -> {
          zkClient.getData(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, null, null, false);
        });
    }

    // 'superUser2' should still have access to everything ... as should newly minted 'superUser3'
    for (String user : Arrays.asList("superUser2", "superUser3")) {
      useAllAccessCredentials(user);
      try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
        for (String path : Arrays.asList(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, "/protectedCreateNode", "/unprotectedCreateNode")) {
          assertNotNull(zkClient.getData(path, null, null, false));
        }
      }
    }
  }
  
  protected static void doTest(
      SolrZkClient zkClient,
      boolean getData, boolean list, boolean create, boolean setData, boolean delete,
      boolean secureGet, boolean secureList, boolean secureCreate, boolean secureSet, boolean secureDelete) throws Exception {
    doTest(zkClient, "/protectedCreateNode", getData, list, create, setData, delete);
    doTest(zkClient, "/protectedMakePathNode", getData, list, create, setData, delete);
    doTest(zkClient, "/unprotectedCreateNode", true, true, true, true, delete);
    doTest(zkClient, "/unprotectedMakePathNode", true, true, true, true, delete);
    doTest(zkClient, SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, secureGet, secureList, secureCreate, secureSet, secureDelete);
  }
  
  protected static void doTest(SolrZkClient zkClient, String path, boolean getData, boolean list, boolean create, boolean setData, boolean delete) throws Exception {
    try {
      zkClient.getData(path, null, null, false);
      if (!getData) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (getData) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.getChildren(path, null, false);
      if (!list) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (list) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.create(path + "/subnode", null, CreateMode.PERSISTENT, false);
      if (!create) fail("NoAuthException expected ");
      else {
        zkClient.delete(path + "/subnode", -1, false);
      }
    } catch (NoAuthException nae) {
      if (create) {
        nae.printStackTrace();
        fail("No NoAuthException expected");
      }
      // expected
    }
    
    try {
      zkClient.makePath(path + "/subnode/subsubnode", false);
      if (!create) fail("NoAuthException expected ");
      else {
        zkClient.delete(path + "/subnode/subsubnode", -1, false);
        zkClient.delete(path + "/subnode", -1, false);
      }
    } catch (NoAuthException nae) {
      if (create) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.setData(path, (byte[])null, false);
      if (!setData) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (setData) fail("No NoAuthException expected");
      // expected
    }

    try {
      // Actually about the ACLs on /solr, but that is protected
      zkClient.delete(path, -1, false);
      if (!delete) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (delete) fail("No NoAuthException expected");
      // expected
    }

  }
  
  private void useNoCredentials() {
    clearSecuritySystemProperties();
  }
  
  private void useWrongCredentials() throws Exception {
    clearSecuritySystemProperties();
    
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    Properties props = new Properties();
    
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "solradmin");
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "solradmin_WRONG_Password");

    setCredentialProps(props);
  }

  private void useAllAccessCredentials(final String... creds) throws Exception {
    clearSecuritySystemProperties();
    
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Properties props = new Properties();

    // suffix shouldn't matter as long as they user/pass are consistent between user & pass...
    
    int aclId = 42;
    for (String acl : creds) {
      props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME + "." + aclId, acl);
      props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME + "." + aclId, acl + "Password");
      aclId++;
    }
    
    props.put(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME+".YAK", "readonlyACLUsername");
    props.put(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME+".YAK", "readonlyACLPassword");

    setCredentialProps(props);
    
  }
  
  private void useReadonlyCredentials() throws Exception {
    clearSecuritySystemProperties();

    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Properties props = new Properties();

    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "readonlyACLUsername");
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "readonlyACLPassword");

    setCredentialProps(props);
  }

  private void setSecuritySystemProperties() throws Exception {
    clearSecuritySystemProperties();
    
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    
    Properties props = new Properties();
    
    // suffix shouldn't matter as long as they user/pass are consistent between user & pass...
    
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "solradmin");
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "solradminPassword");

    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME+".ZOT", "superUser2");
    props.put(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME+".ZOT", "superUser2Password");
    
    props.put(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME+".ABC", "readonlyACLUsername");
    props.put(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME+".ABC", "readonlyACLPassword");
    
    setCredentialProps(props);
  }
  
  private void clearSecuritySystemProperties() {
    System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME);
    
    for (String p : System.getProperties().stringPropertyNames()) {
      for (String prefix : Arrays.asList(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
                                         VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
                                         VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
                                         VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME)) {
        if (p.startsWith(prefix)) {
          System.clearProperty(p);
          break;
        }
      }
    }
  }
  
}
