package it.pagopa.util.cosmos_copy.model;

public record SyncStatus(
        boolean running,
        long processedDocuments,
        Long targetLimit
) {}