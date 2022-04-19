package org.apache.kafka.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum MetadataVersion {
    UNINITIALIZED(-1),
    // TODO all the other versions
    IBP_2_7_IV0(-1),
    IBP_2_7_IV1(-1),
    IBP_2_7_IV2(-1),
    IBP_2_8_IV0(-1),
    IBP_2_8_IV1(-1),
    // KRaft preview
    IBP_3_0_IV0(1),
    IBP_3_0_IV1(2),
    IBP_3_1_IV0(3),
    IBP_3_2_IV0(4),
    // KRaft GA
    IBP_3_3_IV0(5)
    ;

    private static final Map<String, MetadataVersion> ibpVersions;
    static {{
        ibpVersions = new HashMap<>();
        Pattern versionPattern = Pattern.compile("^IBP_(\\d)_(\\d)_IV(\\d)$");
        Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
        for (MetadataVersion version : MetadataVersion.values()) {
            Matcher matcher = versionPattern.matcher(version.name());
            if (matcher.find()) {
                int major = Integer.parseInt(matcher.group(1));
                int minor = Integer.parseInt(matcher.group(2));
                int iv = Integer.parseInt(matcher.group(3));
                String normalizedVersion = String.format("%d.%d_IV%d", major, minor, iv);
                maxInterVersion.compute(String.format("%d.%d", major, minor), (__, currentVersion) -> {
                    if (currentVersion == null) {
                        return version;
                    } else if (version.compareTo(currentVersion) > 0) {
                        return version;
                    } else {
                        return currentVersion;
                    }
                });
                ibpVersions.put(normalizedVersion, version);
            }
        }

        ibpVersions.putAll(maxInterVersion);
    }}

    private final Optional<Short> metadataVersion;
    private final boolean changedMetadata;

    MetadataVersion(int metadataVersion) {
        this(metadataVersion, true);
    }

    MetadataVersion(int metadataVersion, boolean changedMetadata) {
        if (metadataVersion > 0) {
            this.metadataVersion = Optional.of((short) metadataVersion);
        } else {
            this.metadataVersion = Optional.empty();
        }
        this.changedMetadata = changedMetadata;
    }

    public static MetadataVersion latest() {
        MetadataVersion[] values = MetadataVersion.values();
        return values[values.length - 1];
    }

    public static MetadataVersion stable() {
        return MetadataVersion.IBP_3_3_IV0;
    }

    public Optional<Short> metadataVersion() {
        return metadataVersion;
    }

    public boolean changedMetadata() {
        return changedMetadata;
    }

    public Optional<MetadataVersion> previousVersion() {
        int idx = this.ordinal();
        if (idx == 0) {
            return Optional.empty();
        } else {
            return Optional.of(MetadataVersion.values()[idx - 1]);
        }
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
        while (!version.changedMetadata() && version != lowVersion) {
            Optional<MetadataVersion> prev = version.previousVersion();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version != targetVersion;
    }

    public static void main(String[] args) {
        System.err.println(MetadataVersion.latest());
    }
}
