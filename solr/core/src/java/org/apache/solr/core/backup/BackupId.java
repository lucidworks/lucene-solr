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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.core.backup.BackupManager.ZK_STATE_DIR;

public class BackupId implements Comparable<BackupId>{
    private static final Pattern BACKUP_PROPS_ID_PTN = Pattern.compile("backup_([0-9]+).properties");

    public final int id;

    public BackupId(int id) {
        this.id = id;
    }

    public static BackupId zero() {
        return new BackupId(0);
    }

    public static BackupId oldVersion() {
        return new BackupId(-1);
    }

    public BackupId nextBackupId() {
        return new BackupId(id+1);
    }

    public static List<BackupId> findAll(String[] listFiles) {
        List<BackupId> result = new ArrayList<>();
        for (String file: listFiles) {
            Matcher m = BACKUP_PROPS_ID_PTN.matcher(file);
            if (m.find()) {
                result.add(new BackupId(Integer.parseInt(m.group(1))));
            }
        }

        return result;
    }

    public String getZkStateDir() {
        if (id == -1) {
            return ZK_STATE_DIR;
        }
        return String.format(Locale.ROOT, "%s_%d/", ZK_STATE_DIR, id);
    }

    public String getBackupPropsName() {
        if (id == -1) {
            return BackupManager.BACKUP_PROPS_FILE;
        }
        return getBackupPropsName(id);
    }

    private static String getBackupPropsName(int id) {
        return String.format(Locale.ROOT, "backup_%d.properties", id);
    }

    static Optional<BackupId> findMostRecent(String[] listFiles) {
        return findAll(listFiles).stream().max(Comparator.comparingInt(o -> o.id));
    }

    public int getId() {
        return id;
    }

    @Override
    public int compareTo(BackupId o) {
        return Integer.compare(this.id, o.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupId backupId = (BackupId) o;
        return id == backupId.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
