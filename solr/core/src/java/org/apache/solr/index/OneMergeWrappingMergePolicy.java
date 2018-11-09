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

import java.io.IOException;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

/**
 * A wrapping merge policy that wraps the {@link org.apache.lucene.index.MergePolicy.OneMerge}
 * objects returned by the wrapped merge policy.
 *
 * @lucene.experimental
 */
public class OneMergeWrappingMergePolicy extends FilterMergePolicy {

  private final UnaryOperator<MergePolicy.OneMerge> wrapOneMerge;

  /**
   * Constructor
   *
   * @param in - the wrapped merge policy
   * @param wrapOneMerge - operator for wrapping OneMerge objects
   */
  public OneMergeWrappingMergePolicy(MergePolicy in, UnaryOperator<MergePolicy.OneMerge> wrapOneMerge) {
    super(in);
    this.wrapOneMerge = wrapOneMerge;
  }

  @Override
  public MergePolicy.MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return wrapSpec(in.findMerges(mergeTrigger, segmentInfos, writer));
  }

  @Override
  public MergePolicy.MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                                         Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    return wrapSpec(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer));
  }

  @Override
  public MergePolicy.MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return wrapSpec(in.findForcedDeletesMerges(segmentInfos, writer));
  }

  private MergePolicy.MergeSpecification wrapSpec(MergePolicy.MergeSpecification spec) {
    MergePolicy.MergeSpecification wrapped = spec == null ? null : new MergePolicy.MergeSpecification();
    if (wrapped != null) {
      for (MergePolicy.OneMerge merge : spec.merges) {
        wrapped.add(wrapOneMerge.apply(merge));
      }
    }
    return wrapped;
  }

}

