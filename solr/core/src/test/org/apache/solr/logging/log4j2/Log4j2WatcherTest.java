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
package org.apache.solr.logging.log4j2;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LoggerInfo;
import org.junit.Test;

public class Log4j2WatcherTest extends LuceneTestCase {

  @Test
  public void testLog4j2WatcherImpl() throws Exception {
    Log4j2Watcher watcher = new Log4j2Watcher();

    assertEquals("Log4j2", watcher.getName());

    watcher.registerListener(new ListenerConfig());

    assertEquals("Expected initial threshold to be WARN", "WARN", watcher.getThreshold());

    String newThreshold = Level.INFO.toString();
    watcher.setThreshold(newThreshold);
    assertEquals("Expected threshold to be " + newThreshold + " after update", newThreshold, watcher.getThreshold());

    Collection<LoggerInfo> loggers = watcher.getAllLoggers();
    assertNotNull(loggers);
    assertTrue(!loggers.isEmpty());

    List<String> allLevels = watcher.getAllLevels();
    for (LoggerInfo info : loggers) {
      String randomLevel = allLevels.get(random().nextInt(allLevels.size()));
      watcher.setLogLevel(info.getName(), randomLevel);
      assertEquals(randomLevel, LogManager.getLogger(info.getName()).getLevel().toString());
    }

    // so our history only has 1 doc in it
    long since = 0;
    Thread.sleep(1000);

    newThreshold = Level.WARN.toString();
    watcher.setThreshold(newThreshold);
    assertEquals("Expected threshold to be " + newThreshold + " after update", newThreshold, watcher.getThreshold());

    Logger solrLog = LogManager.getLogger("org.apache.solr");
    watcher.setLogLevel(solrLog.getName(), Level.WARN.toString());
    assertEquals(Level.WARN.toString(), String.valueOf(solrLog.getLevel()));
    String warnMsg = "This is a warn message.";
    solrLog.warn(warnMsg);

    SolrDocumentList history = watcher.getHistory(since, new AtomicBoolean());
    assertTrue(history.getNumFound() >= 1);

    int foundMsg = 0;
    for (SolrDocument next : history) {
      if (solrLog.getName().equals(next.getFirstValue("logger"))) {
        ++foundMsg;

        assertNotNull(next);
        assertNotNull(next.getFirstValue("time"));
        assertEquals(warnMsg, next.getFirstValue("message"));
      }
    }
    assertTrue("Test warn message not captured by the LogWatcher as it should have been; history=" + history, foundMsg == 1);
  }
}