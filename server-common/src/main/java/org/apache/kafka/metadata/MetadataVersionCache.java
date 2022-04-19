package org.apache.kafka.metadata;

import java.util.function.Supplier;

public class MetadataVersionCache implements Supplier<MetadataVersion> {
    private MetadataVersion currentVersion;

    @Override
    public MetadataVersion get() {
        return currentVersion;
    }
}
