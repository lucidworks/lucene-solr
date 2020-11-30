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

package org.apache.solr.core.backup;

import java.util.Optional;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class BackupIdTest extends SolrTestCase {

    @Test
    public void test() {
        BackupId backupId = BackupId.findMostRecent(new String[] {"aaa", "baa.properties", "backup.properties",
                "backup_1.properties", "backup_2.properties", "backup_neqewq.properties", "backup999.properties"}).get();
        assertEquals("backup_2.properties", backupId.getBackupPropsName());
        backupId = backupId.nextBackupId();
        assertEquals("backup_3.properties", backupId.getBackupPropsName());

        Optional<BackupId> op = BackupId.findMostRecent(new String[0]);
        assertFalse(op.isPresent());
    }
}
