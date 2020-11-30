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

package org.apache.solr.handler;

import java.io.IOException;
import java.net.URI;

import org.apache.solr.core.backup.repository.BackupRepository;

/**
 * Utility class for getting paths related to incremental backup
 */
public class IncrementalBackupPaths {

    private BackupRepository repository;
    private URI backupLoc;

    public IncrementalBackupPaths(BackupRepository repository, URI backupLoc) {
        this.repository = repository;
        this.backupLoc = backupLoc;
    }

    public URI getIndexDir() {
        return repository.resolve(backupLoc, "index");
    }

    public URI getShardBackupIdDir() {
        return repository.resolve(backupLoc, "shard_backup_ids");
    }

    public URI getBackupLocation() {
        return backupLoc;
    }

    public void createFolders() throws IOException {
        if (!repository.exists(backupLoc)) {
            repository.createDirectory(backupLoc);
        }
        URI indexDir = getIndexDir();
        if (!repository.exists(indexDir)) {
            repository.createDirectory(indexDir);
        }

        URI shardBackupIdDir = getShardBackupIdDir();
        if (!repository.exists(shardBackupIdDir)) {
            repository.createDirectory(shardBackupIdDir);
        }
    }
}
