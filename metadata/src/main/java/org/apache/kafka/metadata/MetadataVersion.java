/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.metadata;

import java.util.Optional;


/**
 * An enumeration of the valid metadata versions for the cluster.
 */
public enum MetadataVersion {

    UNINITIALIZED(-1),
    IBP_2_7_IV1(-1, true),
    // KRaft preview versions
    IBP_3_0_IV0(1, true),
    IBP_3_0_IV1(2, false),
    IBP_3_1_IV0(3, false),
    IBP_3_2_IV0(4, false),
    // KRaft GA
    IBP_3_3_IV0(5, false);

    public static final String FEATURE_NAME = "metadata.version";

    private final short version;
    private final boolean didMetadataChange;

    MetadataVersion(int version) {
        this(version, true);
    }

    MetadataVersion(int version, boolean didMetadataChange) {
        if (version > Short.MAX_VALUE || version < Short.MIN_VALUE) {
            throw new IllegalArgumentException("version must be a short");
        }
        this.version = (short) version;
        this.didMetadataChange = didMetadataChange;
    }

    public static MetadataVersion fromValue(short value) {
        for (MetadataVersion version : MetadataVersion.values()) {
            if (version.version == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unsupported metadata.version " + value + "!");
    }

    public static MetadataVersion stable() {
        return IBP_3_2_IV0;
    }

    public static MetadataVersion latest() {
        MetadataVersion[] values = MetadataVersion.values();
        return values[values.length - 1];
    }

    public static boolean checkIfMetadataChanged(MetadataVersion sourceVersion, MetadataVersion targetVersion) {
        if (sourceVersion == targetVersion) {
            return false;
        }

        final MetadataVersion highVersion, lowVersion;
        if (sourceVersion.compareTo(targetVersion) < 0) {
            highVersion = targetVersion;
            lowVersion = sourceVersion;
        } else {
            highVersion = sourceVersion;
            lowVersion = targetVersion;
        }
        MetadataVersion version = highVersion;
        while (!version.didMetadataChange() && version != lowVersion) {
            Optional<MetadataVersion> prev = version.previous();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version != targetVersion;
    }


    public short version() {
        return version;
    }

    public Optional<MetadataVersion> previous() {
        int idx = this.ordinal();
        if (idx > 1) {
            return Optional.of(MetadataVersion.values()[idx - 1]);
        } else {
            return Optional.empty();
        }
    }

    public boolean didMetadataChange() {
        return didMetadataChange;
    }
}
