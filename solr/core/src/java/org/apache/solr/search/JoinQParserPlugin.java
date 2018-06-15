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
package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.join.GraphEdgeCollector;
import org.apache.solr.search.join.GraphPointsCollector;
import org.apache.solr.search.join.GraphTermsCollector;
import org.apache.solr.search.join.ScoreJoinQParserPlugin;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;

public class JoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "join";

  public enum JoinMethod {
    DV,  // Collects all from candidates and searches them in one query against the to field
    ENUM, // TermsEnum approach where we iterate over every term and cross check
    SMART // let solr decide the algorithm
    ;

    public static JoinQParserPlugin.JoinMethod fromString(String method) {
      if (method == null || method.length()==0) return DEFAULT_METHOD;
      switch (method) {
        case "dv": return DV;
        case "enum": return ENUM;
        case "smart": return SMART;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown Join method " + method);
      }
    }

    final static JoinQParserPlugin.JoinMethod DEFAULT_METHOD = SMART;
  }

  private static final int NUM_DOCS_THRESHOLD = 500_000;

  public static JoinQParserPlugin.JoinMethod chooseJoinMethod(SolrIndexSearcher searcher, SchemaField fromSchemaField) {
    return chooseJoinMethod(searcher, fromSchemaField, -1);
  }

  /**
   * Call this method when the user has not specified a join method.
   *
   * This is how we choose a method:
   * 1. If the field doesn't have docValues defined then we always use the enum based approach.
   *    This will fail a validation check for point fields as the approach is not supported for point fields.
   * 2. Based on the number of eligible documents :
   *    The JSON Facet knows the "domain" size ( number of docs ) on which the join will be executed.
   *    We pick the DV approach if the number of docs is less than NUM_DOCS_THRESHOLD.
   * 3. If the number of unique values in the "from" field is less than 10% of the index size
   *    then we choose the enum based approach.
   *
   * @param searcher A {@link SolrIndexSearcher} object
   * @param fromSchemaField The schema definition of the "from" field
   * @param domainSize If we know the number of documents matching on which the join is performed.
   *                   -1 if we don't have this information
   * @return The JoinMethod
   *
   */
  public static JoinMethod chooseJoinMethod(SolrIndexSearcher searcher, SchemaField fromSchemaField, int domainSize) {
    if (!fromSchemaField.hasDocValues()) {
      return JoinMethod.ENUM;
    }

    if (fromSchemaField.getType().isPointField()) {
      return JoinMethod.DV; // only dv supported
    }

    if (domainSize > 0 && domainSize < NUM_DOCS_THRESHOLD) {
      //naive estimation based on domain size. This information is not present when executing joins directly
      return JoinMethod.DV;
    }

    List<LeafReaderContext> leaves = searcher.getLeafContexts();
    long[] numDocsPerLeaf = new long[leaves.size()];
    long[] cardinalityPerLeaf = new long[leaves.size()];
    for (int i=0; i<leaves.size(); i++) {
      try {
        numDocsPerLeaf[i] = leaves.get(i).reader().maxDoc();
        cardinalityPerLeaf[i] = DocValues.getSortedSet(leaves.get(i).reader(), fromSchemaField.getName()).getValueCount();
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }
    return chooseJoinMethodByDvStats(numDocsPerLeaf, cardinalityPerLeaf);
  }

  public static JoinMethod chooseJoinMethodByDvStats(long[] numDocsPerLeaf, long[] cardinalityPerLeaf) {
    assert numDocsPerLeaf.length == cardinalityPerLeaf.length;
    if (numDocsPerLeaf.length == 0) {
      return JoinMethod.ENUM;
    }
    double sumRatio = 0;
    for (int i=0; i<numDocsPerLeaf.length; i++) {
      sumRatio += (double) cardinalityPerLeaf[i] / numDocsPerLeaf[i];
    }
    double ratio = sumRatio / numDocsPerLeaf.length;
    if (ratio <= .1) { //low cardinality
      return JoinMethod.ENUM;
    } else {
      return JoinMethod.DV;
    }
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {

      @Override
      public Query parse() throws SyntaxError {
        if (localParams!=null && localParams.get(ScoreJoinQParserPlugin.SCORE)!=null) {
          if (localParams.get("method") != null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "method param is not supported in conjunction with the score param");
          }
          return new ScoreJoinQParserPlugin().createParser(qstr, localParams, params, req).parse();
        } else{
          return parseJoin();
        }
      }
      
      Query parseJoin() throws SyntaxError {
        final String fromField = getParam("from");
        final String fromIndex = getParam("fromIndex");
        final String toField = getParam("to");
        final String v = localParams.get("v");
        JoinMethod method = JoinMethod.fromString(getParam("method"));

        final String coreName;

        Query fromQuery;
        long fromCoreOpenTime = 0;

        if (fromIndex != null && !fromIndex.equals(req.getCore().getCoreDescriptor().getName()) ) {
          CoreContainer container = req.getCore().getCoreContainer();

          // if in SolrCloud mode, fromIndex should be the name of a single-sharded collection
          coreName = ScoreJoinQParserPlugin.getCoreName(fromIndex, container);

          final SolrCore fromCore = container.getCore(coreName);
          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Cross-core join: no such core " + coreName);
          }

          RefCounted<SolrIndexSearcher> fromHolder = null;
          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
          try {
            QParser parser = QParser.getParser(v, otherReq);
            fromQuery = parser.getQuery();
            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) fromCoreOpenTime = fromHolder.get().getOpenNanoTime();
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          coreName = null;
          QParser fromQueryParser = subQuery(v, null);
          fromQueryParser.setIsFilter(true);
          fromQuery = fromQueryParser.getQuery();
        }

        JoinQuery jq = new JoinQuery(fromField, toField, coreName == null ? fromIndex : coreName, fromQuery, method, null);
        jq.fromCoreOpenTime = fromCoreOpenTime;
        return jq;
      }
    };
  }

  /**
   * A helper method for other plugins to create (non-scoring) JoinQueries wrapped around arbitrary queries against the same core.
   * 
   * @param subQuery the query to define the starting set of documents on the "left side" of the join
   * @param fromField "left side" field name to use in the join
   * @param toField "right side" field name to use in the join
   * @param method use the graph collector algorithm VS terms enum
   * @param domainFilters Filter candidate documents that need to be returned after the join operation.
   *                      The provided filters can be reused. Pass null if you want all the matches
   */
  public static Query createJoinQuery(Query subQuery, String fromField, String toField, JoinMethod method, List<Query> domainFilters) {
    List<Query> queryFilters = null;
    //TODO We should not expose domainFilters to join field like I explored in this patch. It gives us less flexibility on what to cache
    //need to do more research if it's helpful or not to expose ( are we leap frogging by execute this with the main query? )
    if (domainFilters != null) {
      queryFilters = new ArrayList<>(domainFilters.size());
      for (Query domainFilter : domainFilters) {
        WrappedQuery q = new WrappedQuery(domainFilter);
        q.setCache(false);
        queryFilters.add(q);
      }
    }
    return new JoinQuery(fromField, toField, null, subQuery, method, queryFilters);
  }
  
}


class JoinQuery extends Query {
  String fromField;
  String toField;
  String fromIndex; // TODO: name is missleading here compared to JoinQParserPlugin usage - here it must be a core name
  Query q;
  long fromCoreOpenTime;
  JoinQParserPlugin.JoinMethod method;
  final List<Query> domainFilters;

  public JoinQuery(String fromField, String toField, String coreName, Query subQuery, JoinQParserPlugin.JoinMethod method, List<Query> domainFilters) {
    assert null != fromField;
    assert null != toField;
    assert null != subQuery;
    
    this.fromField = fromField;
    this.toField = toField;
    this.q = subQuery;
    this.method = method;
    this.domainFilters = domainFilters;
    
    this.fromIndex = coreName; // may be null
  }

  public Query getQuery() { return q; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // don't rewrite the subQuery
    return super.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new JoinQueryWeight((SolrIndexSearcher)searcher, boost);
  }

  private class JoinQueryWeight extends ConstantScoreWeight {
    SolrIndexSearcher fromSearcher;
    RefCounted<SolrIndexSearcher> fromRef;
    SolrIndexSearcher toSearcher;
    private Similarity similarity;
    ResponseBuilder rb;

    public JoinQueryWeight(SolrIndexSearcher searcher, float boost) {
      super(JoinQuery.this, boost);
      this.fromSearcher = searcher;
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        rb = info.getResponseBuilder();
      }

      if (fromIndex == null) {
        this.fromSearcher = searcher;
      } else {
        if (info == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join must have SolrRequestInfo");
        }

        CoreContainer container = searcher.getCore().getCoreContainer();
        final SolrCore fromCore = container.getCore(fromIndex);

        if (fromCore == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
        }

        if (info.getReq().getCore() == fromCore) {
          // if this is the same core, use the searcher passed in... otherwise we could be warming and
          // get an older searcher from the core.
          fromSearcher = searcher;
        } else {
          // This could block if there is a static warming query with a join in it, and if useColdSearcher is true.
          // Deadlock could result if two cores both had useColdSearcher and had joins that used eachother.
          // This would be very predictable though (should happen every time if misconfigured)
          fromRef = fromCore.getSearcher(false, true, null);

          // be careful not to do anything with this searcher that requires the thread local
          // SolrRequestInfo in a manner that requires the core in the request to match
          fromSearcher = fromRef.get();
        }

        if (fromRef != null) {
          final RefCounted<SolrIndexSearcher> ref = fromRef;
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              ref.decref();
            }
          });
        }

        info.addCloseHook(new Closeable() {
          @Override
          public void close() {
            fromCore.close();
          }
        });

      }
      this.toSearcher = searcher;
    }

    DocSet resultSet;
    Filter filter;


    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (filter == null) {
        boolean debug = rb != null && rb.isDebug();
        RTimer timer = (debug ? new RTimer() : null);
        resultSet = getDocSet();
        if (timer != null) timer.stop();

        if (debug) {
          SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
          dbg.add("time", (long) timer.getTime());
          dbg.add("fromSetSize", fromSetSize);  // the input
          dbg.add("toSetSize", resultSet.size());    // the output

          dbg.add("fromTermCount", fromTermCount);
          dbg.add("fromTermTotalDf", fromTermTotalDf);
          dbg.add("fromTermDirectCount", fromTermDirectCount);
          dbg.add("fromTermHits", fromTermHits);
          dbg.add("fromTermHitsTotalDf", fromTermHitsTotalDf);
          dbg.add("toTermHits", toTermHits);
          dbg.add("toTermHitsTotalDf", toTermHitsTotalDf);
          dbg.add("toTermDirectCount", toTermDirectCount);
          dbg.add("smallSetsDeferred", smallSetsDeferred);
          dbg.add("toSetDocsAdded", resultListDocs);

          // TODO: perhaps synchronize  addDebug in the future...
          rb.addDebug(dbg, "join", JoinQuery.this.toString());
        }

        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, null);
      if (readerSet == null) {
        return null;
      }
      DocIdSetIterator readerSetIterator = readerSet.iterator();
      if (readerSetIterator == null) {
        return null;
      }
      return new ConstantScoreScorer(this, score(), readerSetIterator);
    }


    // most of these statistics are only used for the enum method
    int fromSetSize;          // number of docs in the fromSet (that match the from query)
    long resultListDocs;      // total number of docs collected
    int fromTermCount;
    long fromTermTotalDf;
    int fromTermDirectCount;  // number of fromTerms that were too small to use the filter cache
    int fromTermHits;         // number of fromTerms that intersected the from query
    long fromTermHitsTotalDf; // sum of the df of the matching terms
    int toTermHits;           // num if intersecting from terms that match a term in the to field
    long toTermHitsTotalDf;   // sum of the df for the toTermHits
    int toTermDirectCount;    // number of toTerms that we set directly on a bitset rather than doing set intersections
    int smallSetsDeferred;    // number of small sets collected to be used later to intersect w/ bitset or create another small set


    public DocSet getDocSet() throws IOException {
      SchemaField fromSchemaField = fromSearcher.getSchema().getField(fromField);
      SchemaField toSchemaField = toSearcher.getSchema().getField(toField);

      validateJoinMethod(method, fromSchemaField);
      if (method == JoinQParserPlugin.JoinMethod.SMART) {
        method = JoinQParserPlugin.chooseJoinMethod(fromSearcher, fromSchemaField);
      }


      if (method == JoinQParserPlugin.JoinMethod.DV) {
        GraphEdgeCollector collector;
        if (toSchemaField.getType().isPointField()) {
          collector = new GraphPointsCollector(fromSchemaField, null, null);
        } else {
          collector = new GraphTermsCollector(fromSchemaField, null, null);
        }
        return executeJoinQuery(collector, toSchemaField);
      }

      return getDocSetEnumerate();
    }

    public void validateJoinMethod(JoinQParserPlugin.JoinMethod method, SchemaField schemaField) {
      if (method == JoinQParserPlugin.JoinMethod.DV) {
        //field must have docValues enabled for method=dv
        if (!schemaField.hasDocValues()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "method=dv only works for fields with docValues=true");
        }
      }

      if (schemaField.getType().isPointField()) {
        if (!schemaField.hasDocValues()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Point fields must have docValues=true");
        }
        if (method == JoinQParserPlugin.JoinMethod.ENUM && schemaField.getType().isPointField()) {
          //Is a user explicitly asks for method=enum then throw error if it's a point field as it's not supported
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "method=enum not supported on a point field");
        }
      }

    }

    /**
     * This method executes the join by collecting all eligible values in the "from" field and then firing a query
     * against the "to" field to get the matching documents applying any filters specified to speed up execution
     *
     * If the number of matches in the "from" field is very high ( for example above 10k ) then this could potentially
     * be slower than it's alternate solutions
     *
     * See {@link GraphEdgeCollector} for more performance characteristics
     *
     * @param collector a graph collector based on the field type.
     * @param toSchemaField Schema definition for the "to" field
     * @return A DocSet after the join and applying any provided filters
     * @throws IOException
     */
    protected DocSet executeJoinQuery(GraphEdgeCollector collector, SchemaField toSchemaField) throws IOException {
      fromSearcher.search(q, collector);
      Query joinQ = collector.getResultQuery(toSchemaField, false);

      if (domainFilters != null) {

        WrappedQuery uncachedQ = new WrappedQuery(joinQ);
        uncachedQ.setCache(false);

        domainFilters.add(uncachedQ);
        DocSet result = joinQ==null ? DocSet.EMPTY : toSearcher.getDocSet(domainFilters);
        return result;
      }

      // don't cache the resulting docSet... the query may be very large.  Better to cache the results of the join query itself
      DocSet result = joinQ==null ? DocSet.EMPTY : toSearcher.getDocSetNC(joinQ, null);
      return result;
    }



    public DocSet getDocSetEnumerate() throws IOException {
      FixedBitSet resultBits = null;

      // minimum docFreq to use the cache
      int minDocFreqFrom = Math.max(5, fromSearcher.maxDoc() >> 13);
      int minDocFreqTo = Math.max(5, toSearcher.maxDoc() >> 13);

      // use a smaller size than normal since we will need to sort and dedup the results
      int maxSortedIntSize = Math.max(10, toSearcher.maxDoc() >> 10);

      DocSet fromSet = fromSearcher.getDocSet(q);
      fromSetSize = fromSet.size();

      List<DocSet> resultList = new ArrayList<>(10);

      // make sure we have a set that is fast for random access, if we will use it for that
      DocSet fastForRandomSet = fromSet;
      if (minDocFreqFrom>0 && fromSet instanceof SortedIntDocSet) {
        SortedIntDocSet sset = (SortedIntDocSet)fromSet;
        fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
      }


      LeafReader fromReader = fromSearcher.getSlowAtomicReader();
      LeafReader toReader = fromSearcher==toSearcher ? fromReader : toSearcher.getSlowAtomicReader();
      Terms terms = fromReader.terms(fromField);
      Terms toTerms = toReader.terms(toField);
      if (terms == null || toTerms==null) return DocSet.EMPTY;
      String prefixStr = TrieField.getMainValuePrefix(fromSearcher.getSchema().getFieldType(fromField));
      BytesRef prefix = prefixStr == null ? null : new BytesRef(prefixStr);

      BytesRef term = null;
      TermsEnum  termsEnum = terms.iterator();
      TermsEnum  toTermsEnum = toTerms.iterator();
      SolrIndexSearcher.DocsEnumState fromDeState = null;
      SolrIndexSearcher.DocsEnumState toDeState = null;

      if (prefix == null) {
        term = termsEnum.next();
      } else {
        if (termsEnum.seekCeil(prefix) != TermsEnum.SeekStatus.END) {
          term = termsEnum.term();
        }
      }

      Bits fromLiveDocs = fromSearcher.getSlowAtomicReader().getLiveDocs();
      Bits toLiveDocs = fromSearcher == toSearcher ? fromLiveDocs : toSearcher.getSlowAtomicReader().getLiveDocs();

      fromDeState = new SolrIndexSearcher.DocsEnumState();
      fromDeState.fieldName = fromField;
      fromDeState.liveDocs = fromLiveDocs;
      fromDeState.termsEnum = termsEnum;
      fromDeState.postingsEnum = null;
      fromDeState.minSetSizeCached = minDocFreqFrom;

      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.postingsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;

      while (term != null) {
        if (prefix != null && !StringHelper.startsWith(term, prefix))
          break;

        fromTermCount++;

        boolean intersects = false;
        int freq = termsEnum.docFreq();
        fromTermTotalDf++;

        if (freq < minDocFreqFrom) {
          fromTermDirectCount++;
          // OK to skip liveDocs, since we check for intersection with docs matching query
          fromDeState.postingsEnum = fromDeState.termsEnum.postings(fromDeState.postingsEnum, PostingsEnum.NONE);
          PostingsEnum postingsEnum = fromDeState.postingsEnum;

          if (postingsEnum instanceof MultiPostingsEnum) {
            MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
            int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
            outer: for (int subindex = 0; subindex<numSubs; subindex++) {
              MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.postingsEnum == null) continue;
              int base = sub.slice.start;
              int docid;
              while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid+base)) {
                  intersects = true;
                  break outer;
                }
              }
            }
          } else {
            int docid;
            while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (fastForRandomSet.exists(docid)) {
                intersects = true;
                break;
              }
            }
          }
        } else {
          // use the filter cache
          DocSet fromTermSet = fromSearcher.getDocSet(fromDeState);
          intersects = fromSet.intersects(fromTermSet);
        }

        if (intersects) {
          fromTermHits++;
          fromTermHitsTotalDf++;
          TermsEnum.SeekStatus status = toTermsEnum.seekCeil(term);
          if (status == TermsEnum.SeekStatus.END) break;
          if (status == TermsEnum.SeekStatus.FOUND) {
            toTermHits++;
            int df = toTermsEnum.docFreq();
            toTermHitsTotalDf += df;
            if (resultBits==null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
              resultBits = new FixedBitSet(toSearcher.maxDoc());
            }

            // if we don't have a bitset yet, or if the resulting set will be too large
            // use the filterCache to get a DocSet
            if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
              // use filter cache
              DocSet toTermSet = toSearcher.getDocSet(toDeState);
              resultListDocs += toTermSet.size();
              if (resultBits != null) {
                toTermSet.addAllTo(new BitDocSet(resultBits));
              } else {
                if (toTermSet instanceof BitDocSet) {
                  resultBits = ((BitDocSet)toTermSet).bits.clone();
                } else {
                  resultList.add(toTermSet);
                }
              }
            } else {
              toTermDirectCount++;

              // need to use liveDocs here so we don't map to any deleted ones
              toDeState.postingsEnum = toDeState.termsEnum.postings(toDeState.postingsEnum, PostingsEnum.NONE);
              toDeState.postingsEnum = BitsFilteredPostingsEnum.wrap(toDeState.postingsEnum, toDeState.liveDocs);
              PostingsEnum postingsEnum = toDeState.postingsEnum;

              if (postingsEnum instanceof MultiPostingsEnum) {
                MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
                int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
                for (int subindex = 0; subindex<numSubs; subindex++) {
                  MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.postingsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    resultListDocs++;
                    resultBits.set(docid + base);
                  }
                }
              } else {
                int docid;
                while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid);
                }
              }
            }

          }
        }

        term = termsEnum.next();
      }

      smallSetsDeferred = resultList.size();

      if (resultBits != null) {
        BitDocSet bitSet = new BitDocSet(resultBits);
        for (DocSet set : resultList) {
          set.addAllTo(bitSet);
        }
        return bitSet;
      }

      if (resultList.size()==0) {
        return DocSet.EMPTY;
      }

      if (resultList.size() == 1) {
        return resultList.get(0);
      }

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;
      for (DocSet set : resultList) {
        System.arraycopy(((SortedIntDocSet)set).getDocs(), 0, docs, pos, set.size());
        pos += set.size();
      }
      Arrays.sort(docs);
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }

      return new SortedIntDocSet(dedup, dedup.length);
    }

  }

  @Override
  public String toString(String field) {
    return "{!join from="+fromField+" to="+toField
        + (fromIndex != null ? " fromIndex="+fromIndex : "")
        +"}"+q.toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(JoinQuery other) {
    return this.fromField.equals(other.fromField)
        && this.toField.equals(other.toField)
        && this.q.equals(other.q)
        && Objects.equals(fromIndex, other.fromIndex)
        && this.fromCoreOpenTime == other.fromCoreOpenTime;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = h * 31 + fromField.hashCode();
    h = h * 31 + toField.hashCode();
    h = h * 31 + q.hashCode();
    h = h * 31 + Objects.hashCode(fromIndex);
    h = h * 31 + (int) fromCoreOpenTime;
    return h;
  }

}
