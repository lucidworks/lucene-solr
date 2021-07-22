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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.IOException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;

public class VMParamsSingleSetCredentialsDigestZkCredentialsProvider extends DefaultZkCredentialsProvider {
  public static final String DEFAULT_DIGEST_FILE_VM_PARAM_NAME = "zkDigestCredentialsFile";
  public static final String DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME = "zkDigestUsername";
  public static final String DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME = "zkDigestPassword";
  
  final String zkDigestUsernameVMParamName;
  final String zkDigestPasswordVMParamName;

  final Collection<ZkCredentials> creds;
  
  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider() {
    this(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
  }
  
  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider(String zkDigestUsernameVMParamName, String zkDigestPasswordVMParamName) {
    this.zkDigestUsernameVMParamName = zkDigestUsernameVMParamName;
    this.zkDigestPasswordVMParamName = zkDigestPasswordVMParamName;
    
    final Properties props = filterProps(readCredentialsFile(System.getProperty(DEFAULT_DIGEST_FILE_VM_PARAM_NAME)),
                                                             this.zkDigestUsernameVMParamName,
                                                             this.zkDigestPasswordVMParamName);
    
    final List<ZkCredentials> localCreds = new ArrayList<>();
    this.creds = Collections.unmodifiableList(localCreds);

    try {
      for (String propkey : props.stringPropertyNames()) {
        if (propkey.startsWith(zkDigestUsernameVMParamName)) {
          final String username = props.getProperty(propkey);
          // look for password prop with same suffix as propkey
          final String password = props.getProperty(zkDigestPasswordVMParamName + propkey.substring(zkDigestUsernameVMParamName.length()));
          
          if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            localCreds.add(new ZkCredentials("digest", (username + ":" + password).getBytes("UTF-8")));
          }
        }
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Properties readCredentialsFile(final String credentialsFilePath) {
    Properties props = System.getProperties();
    if (null != credentialsFilePath) {
      try (Reader in = new InputStreamReader(new FileInputStream(credentialsFilePath), StandardCharsets.UTF_8)) {
        props = new Properties(props);
        props.load(in);
      } catch (IOException ioe) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                "Unable to read " + DEFAULT_DIGEST_FILE_VM_PARAM_NAME + ": " + credentialsFilePath,
                                ioe);
      }
    }
    return props;
  }

  public static Properties filterProps(final Properties props, final String... prefixes) {
    Properties results = new Properties();
    for (String propkey : props.stringPropertyNames()) {
      for (String prefix : prefixes) {
        if (propkey.startsWith(prefix)) {
          results.setProperty(propkey, props.getProperty(propkey));
        }
      }
    }
    return results;
  }
  
  @Override
  protected Collection<ZkCredentials> createCredentials() {
    return creds;
  }
}

