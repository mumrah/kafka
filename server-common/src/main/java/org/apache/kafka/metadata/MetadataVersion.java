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

import org.apache.kafka.common.record.RecordVersion;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * An enumeration of the valid metadata versions for the cluster.
 */
public enum MetadataVersion {
    UNINITIALIZED(-1),
    IBP_0_8_0(-1),
    IBP_0_8_1(-1),
    IBP_0_8_2(-1),
    IBP_0_9_0(-1),
    IBP_0_10_0_IV0(-1),
    IBP_0_10_0_IV1(-1),
    IBP_0_10_1_IV0(-1),
    IBP_0_10_1_IV1(-1),
    IBP_0_10_1_IV2(-1),
    IBP_0_10_2_IV0(-1),
    IBP_0_11_0_IV0(-1),
    IBP_0_11_0_IV1(-1),
    IBP_0_11_0_IV2(-1),
    IBP_1_0_IV0(-1),
    IBP_1_1_IV0(-1),
    IBP_2_0_IV0(-1),
    IBP_2_0_IV1(-1),
    IBP_2_1_IV0(-1),
    IBP_2_1_IV1(-1),
    IBP_2_1_IV2(-1),
    IBP_2_2_IV0(-1),
    IBP_2_2_IV1(-1),
    IBP_2_3_IV0(-1),
    IBP_2_3_IV1(-1),
    IBP_2_4_IV0(-1),
    IBP_2_4_IV1(-1),
    IBP_2_5_IV0(-1),
    IBP_2_6_IV0(-1),
    IBP_2_7_IV0(-1),
    IBP_2_7_IV1(-1),
    IBP_2_7_IV2(-1),
    IBP_2_8_IV0(-1),
    IBP_2_8_IV1(-1),
    // KRaft preview
    IBP_3_0_IV0(1, true),
    IBP_3_0_IV1(2, false),
    IBP_3_1_IV0(3, false),
    IBP_3_1_IV1(4, false),
    IBP_3_2_IV0(5, false),
    // KRaft GA
    IBP_3_3_IV0(6, false);

    public static final String FEATURE_NAME = "metadata.version";

    private final Optional<Short> metadataVersion;
    private final boolean didMetadataChange;
    private final String shortVersion;
    private final String version;

    MetadataVersion(int metadataVersion) {
        this(metadataVersion, true);
    }

    MetadataVersion(int metadataVersion, boolean didMetadataChange) {
        this.didMetadataChange = didMetadataChange;

        if (metadataVersion > 0) {
            this.metadataVersion = Optional.of((short) metadataVersion);
        } else {
            this.metadataVersion = Optional.empty();
        }

        if (this.name().equals("UNINITIALIZED")) {
            shortVersion = "";
            version = "";
            return;
        }

        Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
        Matcher matcher = versionPattern.matcher(this.name());
        if (matcher.find()) {
            String withoutIV = matcher.group(1);
            // remove any trailing underscores
            if (withoutIV.endsWith("_")) {
                withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
            }
            shortVersion = withoutIV.replace("_", ".");

            if (matcher.group(2) != null) { // versions less than IBP_0_10_0_IV0 do not have IVs
                version = String.format("%s-IV%s", shortVersion, matcher.group(2));
            } else {
                version = shortVersion;
            }
        } else {
            throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                    + "the accepted pattern.");
        }
    }

    public short kraftVersion() {
        return metadataVersion.get();
    }

    public boolean isKraftVersion() {
        return metadataVersion.isPresent();
    }

    public boolean isAlterIsrSupported() {
        return this.isAtLeast(IBP_2_7_IV2);
    }

    public boolean isAllocateProducerIdsSupported() {
        return this.isAtLeast(IBP_3_0_IV0);
    }

    public boolean isTruncationOnFetchSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isOffsetForLeaderEpochSupported() {
        return this.isAtLeast(IBP_0_11_0_IV2);
    }

    public boolean isTopicIdsSupported() {
        return this.isAtLeast(IBP_2_8_IV0);
    }

    public boolean isFeatureVersioningSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isSaslInterBrokerHandshakeRequestEnabled() {
        return this.isAtLeast(IBP_0_10_0_IV1);
    }

    public RecordVersion recordVersion() {
        if (this.compareTo(IBP_0_9_0) <= 0) { // IBPs up to IBP_0_9_0 use Record Version V0
            return RecordVersion.V0;
        } else if (this.compareTo(IBP_0_10_2_IV0) <= 0) { // IBPs up to IBP_0_10_2_IV0 use V1
            return RecordVersion.V1;
        } else return RecordVersion.V2; // all greater IBPs use V2
    }


    private static final Map<String, MetadataVersion> IBP_VERSIONS;
    static {
        {
            IBP_VERSIONS = new HashMap<>();
            Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
            Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
            for (MetadataVersion version : MetadataVersion.values()) {
                if (version.equals(MetadataVersion.UNINITIALIZED)) {
                    continue;
                }
                Matcher matcher = versionPattern.matcher(version.name());
                if (matcher.find()) {
                    String withoutIV = matcher.group(1);
                    // remove any trailing underscores
                    if (withoutIV.endsWith("_")) {
                        withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
                    }
                    String shortVersion = withoutIV.replace("_", ".");

                    String normalizedVersion;
                    if (matcher.group(2) != null) {
                        normalizedVersion = String.format("%s-IV%s", shortVersion, matcher.group(2));
                    } else {
                        normalizedVersion = shortVersion;
                    }
                    maxInterVersion.compute(shortVersion, (__, currentVersion) -> {
                        if (currentVersion == null) {
                            return version;
                        } else if (version.compareTo(currentVersion) > 0) {
                            return version;
                        } else {
                            return currentVersion;
                        }
                    });
                    IBP_VERSIONS.put(normalizedVersion, version);
                } else {
                    throw new IllegalArgumentException("Metadata version: " + version.name() + " does not fit "
                            + "any of the accepted patterns.");
                }
            }
            IBP_VERSIONS.putAll(maxInterVersion);
        }
    }

    public String shortVersion() {
        return shortVersion;
    }

    public String version() {
        return version;
    }

    /**
     * Return an `ApiVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
     * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `ApiVersion`.
     */
    public static MetadataVersion fromVersionString(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = (versionString.startsWith("0.")) ? 3 : 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(IBP_VERSIONS.get(key)).orElseThrow(() ->
                new IllegalArgumentException("Version " + versionString + " is not a valid version")
        );
    }

    public static MetadataVersion fromFeatureLevel(short version) {
        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            if (metadataVersion.metadataVersion.isPresent() && metadataVersion.metadataVersion.get() == version) {
                return metadataVersion;
            }
        }
        throw new IllegalArgumentException("No MetadataVersion with version " + version);
    }

    /**
     * Return the minimum `MetadataVersion` that supports `RecordVersion`.
     */
    public static MetadataVersion minSupportedFor(RecordVersion recordVersion) {
        switch (recordVersion) {
            case V0:
                return IBP_0_8_0;
            case V1:
                return IBP_0_10_0_IV0;
            case V2:
                return IBP_0_11_0_IV0;
            default:
                throw new IllegalArgumentException("Invalid message format version " + recordVersion);
        }
    }

    public static MetadataVersion latest() {
        MetadataVersion[] values = MetadataVersion.values();
        return values[values.length - 1];
    }

    public static MetadataVersion stable() {
        return MetadataVersion.IBP_3_3_IV0;
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

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    public Optional<MetadataVersion> previous() {
        int idx = this.ordinal();
        if (idx > 2) {
            return Optional.of(MetadataVersion.values()[idx - 1]);
        } else {
            return Optional.empty();
        }
    }

    public boolean didMetadataChange() {
        return didMetadataChange;
    }

    @Override
    public String toString() {
        return version;
    }
}
