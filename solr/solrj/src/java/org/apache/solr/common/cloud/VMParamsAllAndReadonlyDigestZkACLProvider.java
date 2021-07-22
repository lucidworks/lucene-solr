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
package org.apache.solr.common.cloud;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.solr.common.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class VMParamsAllAndReadonlyDigestZkACLProvider extends SecurityAwareZkACLProvider {

  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME = "zkDigestReadonlyUsername";
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME = "zkDigestReadonlyPassword";
  
  final String zkDigestAllUsernameVMParamName;
  final String zkDigestAllPasswordVMParamName;
  final String zkDigestReadonlyUsernameVMParamName;
  final String zkDigestReadonlyPasswordVMParamName;

  final List<ACL> secureACLs;
  final List<ACL> nonSecureACLs;
  
  public VMParamsAllAndReadonlyDigestZkACLProvider() {
    this(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, 
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME
        );
  }
  
  public VMParamsAllAndReadonlyDigestZkACLProvider(String zkDigestAllUsernameVMParamName, String zkDigestAllPasswordVMParamName,
                                                   String zkDigestReadonlyUsernameVMParamName, String zkDigestReadonlyPasswordVMParamName) {
    this.zkDigestAllUsernameVMParamName = zkDigestAllUsernameVMParamName;
    this.zkDigestAllPasswordVMParamName = zkDigestAllPasswordVMParamName;
    this.zkDigestReadonlyUsernameVMParamName = zkDigestReadonlyUsernameVMParamName;
    this.zkDigestReadonlyPasswordVMParamName = zkDigestReadonlyPasswordVMParamName;
    
    final Properties props = VMParamsSingleSetCredentialsDigestZkCredentialsProvider.filterProps
      (VMParamsSingleSetCredentialsDigestZkCredentialsProvider.readCredentialsFile
       (System.getProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME)),
       this.zkDigestAllUsernameVMParamName,
       this.zkDigestAllPasswordVMParamName,
       this.zkDigestReadonlyUsernameVMParamName,
       this.zkDigestReadonlyPasswordVMParamName);
    
    final List<ACL> adminACLs = createACLsFromProperties(ZooDefs.Perms.ALL, props, zkDigestAllUsernameVMParamName, zkDigestAllPasswordVMParamName);
    final List<ACL> readOnlyACLs = createACLsFromProperties(ZooDefs.Perms.READ, props, zkDigestReadonlyUsernameVMParamName, zkDigestReadonlyPasswordVMParamName);

    final List<ACL> mergedACLs = new ArrayList<>(adminACLs);
    mergedACLs.addAll(readOnlyACLs);
    
    this.nonSecureACLs = Collections.unmodifiableList(mergedACLs.isEmpty() ? ZooDefs.Ids.OPEN_ACL_UNSAFE : mergedACLs);
    this.secureACLs = Collections.unmodifiableList(adminACLs.isEmpty() ? ZooDefs.Ids.OPEN_ACL_UNSAFE : adminACLs);
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    return nonSecureACLs;
  }

  /**
   * @return Set of ACLs to return security-related znodes
   */
  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    return secureACLs;
  }

  private List<ACL> createACLsFromProperties(int perms, Properties props, String usernamePropKeyPrefix, String passwordPropKeyPrefix) {
    List<ACL> result = new ArrayList<>();

    for (String propkey : props.stringPropertyNames()) {
      if (propkey.startsWith(usernamePropKeyPrefix)) {
        final String username = props.getProperty(propkey);
        // look for password prop with same suffix as propkey
        final String password = props.getProperty(passwordPropKeyPrefix + propkey.substring(usernamePropKeyPrefix.length()));
      
        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
          result.add(createDigestACL(perms, username, password));
        }
      }
    }

    return result;
  }

  protected ACL createDigestACL(final int perms, final String user, final String pass) {
    try {
      return new ACL(perms, new Id("digest", DigestAuthenticationProvider.generateDigest(user + ":" + pass)));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Note: only used for tests
   */
  protected List<ACL> createACLsToAdd(boolean includeReadOnly,
                                      String digestAllUsername, String digestAllPassword,
                                      String digestReadonlyUsername, String digestReadonlyPassword) {

    List<ACL> result = new ArrayList<ACL>();
    
    // Not to have to provide too much credentials and ACL information to the process it is assumed that you want "ALL"-acls
    // added to the user you are using to connect to ZK (if you are using VMParamsSingleSetCredentialsDigestZkCredentialsProvider)
    if (!StringUtils.isEmpty(digestAllUsername) && !StringUtils.isEmpty(digestAllPassword)) {
      result.add(createDigestACL(ZooDefs.Perms.ALL, digestAllUsername, digestAllPassword));
    }
    
    if (includeReadOnly) {
      // Besides that support for adding additional "READONLY"-acls for another user
      if (!StringUtils.isEmpty(digestReadonlyUsername) && !StringUtils.isEmpty(digestReadonlyPassword)) {
        result.add(createDigestACL(ZooDefs.Perms.READ, digestReadonlyUsername, digestReadonlyPassword));
      }
    }
    
    if (result.isEmpty()) {
      result = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
    
    return result;
  }

}

