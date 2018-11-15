package org.apache.solr.index;

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

import org.apache.lucene.index.SegmentCommitInfo;

/**
 * Expert: Determines whether a segment should be rewritten and modified.
 * <p>
 * WARNING: The merge policy has total control over they rewrite process and must only do "safe" modifications.
 * <p>
 * Any merge policy, including custom merge policies that want to "do things" to the index should implement this
 * interface. An example is AddDocValuesMergePolicy(Factory)
 * <p>
 * There may be certain operations that can be safely done to existing segments without re-indexing. Merge policies
 * that impelment this interface are responsible for insuring that the modifications are, indeed, safe.
 */

public interface RewriteSegments {
  /**
   *
   * @param info A segment, presumably from an optimize/forceMerge operation
   * @return true - if upon examining the segment, whatever modification the merge policy is carrying out should
   *                rewrite the segment and  modify it.
   *         false - the segment is up to date
   */
  public boolean shouldRewrite(SegmentCommitInfo info);
}
