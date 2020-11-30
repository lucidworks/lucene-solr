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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.util.PropertiesInputStream;

/**
 * ShardBackupId is a metadata (json) file storing all the index files needed for a specific backed up commit.
 * To avoid file names duplication between multiple backups and shards,
 * each index file is stored with an uniqueName and the mapping from uniqueName to its original name is also stored here
 */
public class ShardBackupId {
    private Map<String, BackedFile> allFiles = new HashMap<>();
    private List<String> uniqueFileNames = new ArrayList<>();

    public void addBackedFile(String uniqueFileName, String originalFileName, Checksum fileChecksum) {
        addBackedFile(new BackedFile(uniqueFileName, originalFileName, fileChecksum));
    }

    public int numFiles() {
        return uniqueFileNames.size();
    }

    public long totalSize() {
        return allFiles.values().stream().map(bf -> bf.fileChecksum.size).reduce(0L, Long::sum);
    }

    public void addBackedFile(BackedFile backedFile) {
        allFiles.put(backedFile.originalFileName, backedFile);
        uniqueFileNames.add(backedFile.uniqueFileName);
    }

    public Optional<BackedFile> getFile(String originalFileName) {
        return Optional.ofNullable(allFiles.get(originalFileName));
    }

    public List<String> listUniqueFileNames() {
        return Collections.unmodifiableList(uniqueFileNames);
    }

    public static ShardBackupId empty() {
        return new ShardBackupId();
    }

    public static ShardBackupId from(BackupRepository repository, URI dir, String filename) throws IOException {
        if (!repository.exists(repository.resolve(dir, filename))) {
            return null;
        }

        try (IndexInput is = repository.openInput(dir, filename, IOContext.DEFAULT)) {
            return from(new PropertiesInputStream(is));
        }
    }

    /**
     * Storing ShardBackupId at {@code folderURI} with name {@code filename}.
     * If a file already existed there, overwrite it.
     */
    public void store(BackupRepository repository, URI folderURI, String filename) throws IOException {
        URI fileURI = repository.resolve(folderURI, filename);
        if (repository.exists(fileURI)) {
            repository.delete(folderURI, Collections.singleton(filename), true);
        }

        try (OutputStream os = repository.createOutput(repository.resolve(folderURI, filename))) {
            store(os);
        }
    }

    public Collection<String> listOriginalFileNames() {
        return Collections.unmodifiableSet(allFiles.keySet());
    }

    private void store(OutputStream os) throws IOException {
        @SuppressWarnings({"rawtypes"})
        Map<String, Map> map = new HashMap<>();

        for (BackedFile backedFile : allFiles.values()) {
            Map<String, Object> fileMap = new HashMap<>();
            fileMap.put("fileName", backedFile.originalFileName);
            fileMap.put("checksum", backedFile.fileChecksum.checksum);
            fileMap.put("size", backedFile.fileChecksum.size);
            map.put(backedFile.uniqueFileName, fileMap);
        }

        Utils.writeJson(map, os, false);
    }

    private static ShardBackupId from(InputStream is) {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(is);
        ShardBackupId shardBackupId = new ShardBackupId();
        for (String uniqueFileName : map.keySet()) {
            @SuppressWarnings({"unchecked"})
            Map<String, Object> fileMap = (Map<String, Object>) map.get(uniqueFileName);

            String fileName = (String) fileMap.get("fileName");
            long checksum = (long) fileMap.get("checksum");
            long size = (long) fileMap.get("size");
            shardBackupId.addBackedFile(new BackedFile(uniqueFileName, fileName, new Checksum(checksum, size)));
        }

        return shardBackupId;
    }

    public static class BackedFile {
        public final String uniqueFileName;
        public final String originalFileName;
        public final Checksum fileChecksum;

        BackedFile(String uniqueFileName, String originalFileName, Checksum fileChecksum) {
            this.uniqueFileName = uniqueFileName;
            this.originalFileName = originalFileName;
            this.fileChecksum = fileChecksum;
        }
    }
}
