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
    IBP_3_3_IV0(6, true);

    public static final String FEATURE_NAME = "metadata.version";

    private static final Map<String, MetadataVersion> ibpVersions;
    static {{
        ibpVersions = new HashMap<>();
        Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
        Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
        for (MetadataVersion version : MetadataVersion.values()) {
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
                ibpVersions.put(normalizedVersion, version);
            } else {
                throw new IllegalArgumentException("Metadata version: " + version.name() + " does not fit "
                        + "any of the accepted patterns.");
            }
        }
        ibpVersions.putAll(maxInterVersion);
    }}

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

    public static MetadataVersion apply(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int desiredSegments = (versionString.startsWith("0.")) ? 3 : 2;
        int numSegments = Math.min(desiredSegments, versionSegments.length);
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(ibpVersions.get(key)).orElseThrow(() ->
                new IllegalArgumentException("Version " + versionString + "is not a valid version")
        );
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

    public String shortVersionString() {
        Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)");
        Matcher matcher = versionPattern.matcher(this.name());
        if (matcher.find()) {
            String withoutIV = matcher.group(1);
            // remove any trailing underscores
            if (withoutIV.endsWith("_")) {
                withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
            }
            return withoutIV.replace("_", ".");
        } else {
            throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                    + "the accepted pattern.");
        }
    }

    public String versionString() {
        if (this.compareTo(IBP_0_10_0_IV0) < 0) { // versions less than this do not have IV versions
            return shortVersionString();
        } else {
            Pattern ivPattern = Pattern.compile("^IBP_[\\d_]+IV(\\d)");
            Matcher matcher = ivPattern.matcher(this.name());
            if (matcher.find()) {
                return String.format("%s-%s", shortVersionString(), matcher.group(1));
            } else {
                throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                        + "the accepted pattern.");
            }
        }
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
