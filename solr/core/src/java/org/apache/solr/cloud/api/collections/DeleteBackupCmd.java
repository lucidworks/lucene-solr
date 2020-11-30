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

package org.apache.solr.cloud.api.collections;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.BackupIdStats;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.IncrementalBackupPaths;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.backup.BackupManager.COLLECTION_NAME_PROP;
import static org.apache.solr.core.backup.BackupManager.START_TIME_PROP;

public class DeleteBackupCmd implements OverseerCollectionMessageHandler.Cmd {
    private final OverseerCollectionMessageHandler ocmh;

    DeleteBackupCmd(OverseerCollectionMessageHandler ocmh) {
        this.ocmh = ocmh;
    }

    @Override
    public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        String backupLocation = message.getStr(CoreAdminParams.BACKUP_LOCATION);
        String backupName = message.getStr(NAME);
        String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
        int backupId = message.getInt(CoreAdminParams.BACKUP_ID, -1);
        int lastNumBackupPointsToKeep = message.getInt(CoreAdminParams.MAX_NUM_BACKUP, -1);
        boolean purge = message.getBool(CoreAdminParams.PURGE_BACKUP, false);
        if (backupId == -1 && lastNumBackupPointsToKeep == -1 && !purge) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "%s, %s or %s param must be provided", CoreAdminParams.BACKUP_ID, CoreAdminParams.MAX_NUM_BACKUP,
                    CoreAdminParams.PURGE_BACKUP));
        }
        CoreContainer cc = ocmh.overseer.getCoreContainer();
        try (BackupRepository repository = cc.newBackupRepository(repo)) {
            URI location = repository.createURI(backupLocation);
            URI backupPath = repository.resolve(location, backupName);

            if (purge) {
                purge(repository, backupPath, results);
            } else if (backupId != -1){
                deleteBackupId(repository, backupPath, backupId, results);
            } else {
                keepNumberOfBackup(repository, backupPath, lastNumBackupPointsToKeep, results);
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    /**
     * Clean up {@code backupPath} by removing all indexFiles, shardBackupIds, backupIds that are
     * unreachable, uncompleted or corrupted.
     */
    void purge(BackupRepository repository, URI backupPath, @SuppressWarnings({"rawtypes"}) NamedList result) throws IOException {
        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPath);

        IncrementalBackupPaths backupPaths = new IncrementalBackupPaths(repository, backupPath);
        repository.delete(backupPaths.getIndexDir(), purgeGraph.indexFileDeletes, true);
        repository.delete(backupPaths.getShardBackupIdDir(), purgeGraph.shardBackupIdDeletes, true);
        repository.delete(backupPath, purgeGraph.backupIdDeletes, true);

        @SuppressWarnings({"rawtypes"})
        NamedList details = new NamedList();
        details.add("numBackupIds", purgeGraph.backupIdDeletes.size());
        details.add("numShardBackupIds", purgeGraph.shardBackupIdDeletes.size());
        details.add("numIndexFiles", purgeGraph.indexFileDeletes.size());
        result.add("deleted", details);
    }

    /**
     * Keep most recent {@code  maxNumBackup} and delete the rest.
     */
    void keepNumberOfBackup(BackupRepository repository, URI backupPath,
                            int maxNumBackup,
                            @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        List<BackupId> backupIds = BackupId.findAll(repository.listAllOrEmpty(backupPath));
        if (backupIds.size() <= maxNumBackup) {
            return;
        }

        Collections.sort(backupIds);
        List<BackupId> backupIdDeletes = backupIds.subList(0, backupIds.size() - maxNumBackup);
        deleteBackupIds(backupPath, repository, new HashSet<>(backupIdDeletes), results);
    }

    void deleteBackupIds(URI backupPath, BackupRepository repository,
                         Set<BackupId> backupIdsDeletes,
                         @SuppressWarnings({"rawtypes"}) NamedList results) throws IOException {
        IncrementalBackupPaths incBackupFiles = new IncrementalBackupPaths(repository, backupPath);
        URI shardBackupIdDir = incBackupFiles.getShardBackupIdDir();

        Set<String> referencedIndexFiles = new HashSet<>();
        List<Pair<BackupId,String>> shardBackupIdDeletes = new ArrayList<>();


        String[] shardBackupIdFiles = repository.listAllOrEmpty(shardBackupIdDir);
        for (String shardBackupIdFile : shardBackupIdFiles) {
            Optional<BackupId> backupId = BackupProperties.backupIdOfShardBackupId(shardBackupIdFile);
            if (!backupId.isPresent())
                continue;

            if (backupIdsDeletes.contains(backupId.get())) {
                Pair<BackupId, String> pair = new Pair<>(backupId.get(), shardBackupIdFile);
                shardBackupIdDeletes.add(pair);
            } else {
                ShardBackupId shardBackupId = ShardBackupId.from(repository, shardBackupIdDir, shardBackupIdFile);
                if (shardBackupId != null)
                    referencedIndexFiles.addAll(shardBackupId.listUniqueFileNames());
            }
        }


        Map<BackupId, BackupIdStats> backupIdToCollectionBackupPoint = new HashMap<>();
        List<String> unusedFiles = new ArrayList<>();
        for (Pair<BackupId, String> entry : shardBackupIdDeletes) {
            BackupId backupId = entry.first();
            ShardBackupId shardBackupId = ShardBackupId.from(repository, shardBackupIdDir, entry.second());
            if (shardBackupId == null)
                continue;

            backupIdToCollectionBackupPoint
                    .putIfAbsent(backupId, new BackupIdStats());
            backupIdToCollectionBackupPoint.get(backupId).add(shardBackupId);

            for (String uniqueIndexFile : shardBackupId.listUniqueFileNames()) {
                if (!referencedIndexFiles.contains(uniqueIndexFile)) {
                    unusedFiles.add(uniqueIndexFile);
                }
            }
        }

        repository.delete(incBackupFiles.getShardBackupIdDir(),
                shardBackupIdDeletes.stream().map(Pair::second).collect(Collectors.toList()), true);
        repository.delete(incBackupFiles.getIndexDir(), unusedFiles, true);
        try {
            for (BackupId backupId : backupIdsDeletes) {
                repository.deleteDirectory(repository.resolve(backupPath, backupId.getZkStateDir()));
            }
        } catch (FileNotFoundException e) {
            //ignore this
        }

        //add details to result before deleting backupPropFiles
        addResult(backupPath, repository, backupIdsDeletes, backupIdToCollectionBackupPoint, results);
        repository.delete(backupPath, backupIdsDeletes.stream().map(BackupId::getBackupPropsName).collect(Collectors.toList()), true);
    }

    @SuppressWarnings("unchecked")
    private void addResult(URI backupPath, BackupRepository repository,
                           Set<BackupId> backupIdDeletes,
                           Map<BackupId, BackupIdStats> backupIdToCollectionBackupPoint,
                           @SuppressWarnings({"rawtypes"}) NamedList results) {

        String collectionName = null;
        @SuppressWarnings({"rawtypes"})
        List<NamedList> shardBackupIdDetails = new ArrayList<>();
        results.add("deleted", shardBackupIdDetails);
        for (BackupId backupId : backupIdDeletes) {
            NamedList<Object> backupIdResult = new NamedList<>();

            try {
                BackupProperties props = BackupProperties.readFrom(repository, backupPath, backupId.getBackupPropsName());
                backupIdResult.add(START_TIME_PROP, props.getStartTime());
                if (collectionName == null) {
                    collectionName = props.getCollection();
                    results.add(COLLECTION_NAME_PROP, collectionName);
                }
            } catch (IOException e) {
                //prop file not found
            }

            BackupIdStats cbp = backupIdToCollectionBackupPoint.getOrDefault(backupId, new BackupIdStats());
            backupIdResult.add("backupId", backupId.getId());
            backupIdResult.add("size", cbp.getTotalSize());
            backupIdResult.add("numFiles", cbp.getNumFiles());
            shardBackupIdDetails.add(backupIdResult);
        }
    }

    private void deleteBackupId(BackupRepository repository, URI backupPath,
                                int bid, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        BackupId backupId = new BackupId(bid);
        if (!repository.exists(repository.resolve(backupPath, backupId.getBackupPropsName()))) {
            return;
        }

        deleteBackupIds(backupPath, repository, Collections.singleton(backupId), results);
    }

    final static class PurgeGraph {
        // graph
        Map<String, Node> backupIdNodeMap = new HashMap<>();
        Map<String, Node> shardBackupIdNodeMap = new HashMap<>();
        Map<String, Node> indexFileNodeMap = new HashMap<>();

        // delete queues
        List<String> backupIdDeletes = new ArrayList<>();
        List<String> shardBackupIdDeletes = new ArrayList<>();
        List<String> indexFileDeletes = new ArrayList<>();

        public void build(BackupRepository repository, URI backupPath) throws IOException {
            IncrementalBackupPaths backupPaths = new IncrementalBackupPaths(repository, backupPath);
            buildLogicalGraph(repository, backupPath);
            findDeletableNodes(repository, backupPaths);
        }

        public void findDeletableNodes(BackupRepository repository, IncrementalBackupPaths backupPaths) {
            // mark nodes as existing
            visitExistingNodes(repository.listAllOrEmpty(backupPaths.getShardBackupIdDir()),
                    shardBackupIdNodeMap, shardBackupIdDeletes);
            // this may be a long running commands
            visitExistingNodes(repository.listAllOrEmpty(backupPaths.getIndexDir()),
                    indexFileNodeMap, indexFileDeletes);

            // for nodes which are not existing, propagate that information to other nodes
            shardBackupIdNodeMap.values().forEach(Node::propagateNotExisting);
            indexFileNodeMap.values().forEach(Node::propagateNotExisting);

            addDeleteNodesToQueue(backupIdNodeMap, backupIdDeletes);
            addDeleteNodesToQueue(shardBackupIdNodeMap, shardBackupIdDeletes);
            addDeleteNodesToQueue(indexFileNodeMap, indexFileDeletes);
        }

        /**
         * Visiting files (nodes) actually present in physical layer,
         * if it does not present in the {@code nodeMap}, it should be deleted by putting into the {@code deleteQueue}
         */
        private void visitExistingNodes(String[] existingNodeKeys, Map<String, Node> nodeMap, List<String> deleteQueue) {
            for (String nodeKey : existingNodeKeys) {
                Node node = nodeMap.get(nodeKey);

                if (node == null) {
                    deleteQueue.add(nodeKey);
                } else {
                    node.existing = true;
                }
            }
        }

        private <T> void addDeleteNodesToQueue(Map<T, Node> tNodeMap, List<T> deleteQueue) {
            tNodeMap.forEach((key, value) -> {
                if (value.delete) {
                    deleteQueue.add(key);
                }
            });
        }

        Node getBackupIdNode(String backupPropsName) {
            return backupIdNodeMap.computeIfAbsent(backupPropsName, bid -> {
                Node node = new Node();
                node.existing = true;
                return node;
            });
        }

        Node getShardBackupIdNode(String shardBackupId) {
            return shardBackupIdNodeMap.computeIfAbsent(shardBackupId, s -> new Node());
        }

        Node getIndexFileNode(String indexFile) {
            return indexFileNodeMap.computeIfAbsent(indexFile, s -> new IndexFileNode());
        }

        void addEdge(Node node1, Node node2) {
            node1.addNeighbor(node2);
            node2.addNeighbor(node1);
        }

        private void buildLogicalGraph(BackupRepository repository, URI backupPath) throws IOException {
            List<BackupId> backupIds = BackupId.findAll(repository.listAllOrEmpty(backupPath));
            for (BackupId backupId : backupIds) {
                BackupProperties backupProps = BackupProperties.readFrom(repository, backupPath,
                        backupId.getBackupPropsName());

                Node backupIdNode = getBackupIdNode(backupId.getBackupPropsName());
                for (String shardBackupIdFile : backupProps.getAllShardBackupIdFiles()) {
                    Node shardBackupIdNode = getShardBackupIdNode(shardBackupIdFile);
                    addEdge(backupIdNode, shardBackupIdNode);

                    ShardBackupId shardBackupId = ShardBackupId.from(repository, backupPath, shardBackupIdFile);
                    if (shardBackupId == null)
                        continue;

                    for (String indexFile : shardBackupId.listUniqueFileNames()) {
                        Node indexFileNode = getIndexFileNode(indexFile);
                        addEdge(indexFileNode, shardBackupIdNode);
                    }
                }
            }
        }
    }

    //ShardBackupId, BackupId
    static class Node {
        List<Node> neighbors;
        boolean delete = false;
        boolean existing = false;

        void addNeighbor(Node node) {
            if (neighbors == null) {
                neighbors = new ArrayList<>();
            }
            neighbors.add(node);
        }

        void propagateNotExisting() {
            if (existing)
                return;

            if (neighbors != null)
                neighbors.forEach(Node::propagateDelete);
        }

        void propagateDelete() {
            if (delete || !existing)
                return;

            delete = true;
            if (neighbors != null) {
                neighbors.forEach(Node::propagateDelete);
            }
        }
    }

    //IndexFile
    final static class IndexFileNode extends Node {
        int refCount = 0;

        @Override
        void addNeighbor(Node node) {
            super.addNeighbor(node);
            refCount++;
        }

        @Override
        void propagateDelete() {
            if (delete || !existing)
                return;

            refCount--;
            if (refCount == 0) {
                delete = true;
            }
        }
    }
}
