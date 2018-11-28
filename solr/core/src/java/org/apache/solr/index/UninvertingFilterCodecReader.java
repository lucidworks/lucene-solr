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
package org.apache.solr.index;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.Bits;

/**
 * Delegates to an UninvertingReader for fields with docvalues
 *
 * This is going to blow up FieldCache, look into an alternative implementation that uninverts without
 * fieldcache
 */
public class UninvertingFilterCodecReader extends FilterCodecReader {

  private final LeafReader uninvertingReader;
  private final DocValuesProducer docValuesProducer;

  public UninvertingFilterCodecReader(CodecReader in, Map<String,UninvertingReader.Type> uninversionMap,
                                      final boolean skipIntegrityCheck) {
    super(in);

    this.uninvertingReader = UninvertingReader.wrap(in, uninversionMap::get);
    this.docValuesProducer = new DocValuesProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return uninvertingReader.getNumericDocValues(field.name);
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return uninvertingReader.getBinaryDocValues(field.name);
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedDocValues(field.name);
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedNumericDocValues(field.name);
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return uninvertingReader.getSortedSetDocValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        if (!skipIntegrityCheck) {
          uninvertingReader.checkIntegrity();
        }
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Bits getDocsWithField(FieldInfo field) throws IOException {
        return uninvertingReader.getDocsWithField(field.name);
      }


    };
  }

  @Override
  protected void doClose() throws IOException {
    docValuesProducer.close();
    uninvertingReader.close();
    super.doClose();
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    return docValuesProducer;
  }

  @Override
  public FieldInfos getFieldInfos() {
    return uninvertingReader.getFieldInfos();
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }

}
