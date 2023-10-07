CREATE DATABASE IF NOT EXISTS chroma_meta CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS chroma_meta.collections (
    id VARCHAR(36) NOT NULL,
    `name` VARCHAR(256) NOT NULL,
    topic TEXT NOT NULL,
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    UNIQUE (`name`),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS chroma_meta.collection_metadata (
    collection_id VARCHAR(36) NOT NULL,
    `key` VARCHAR(256) NOT NULL,
    str_value TEXT,
    int_value INTEGER,
    float_value REAL,
    ts BIGINT UNSIGNED DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (collection_id, `key`),
    -- FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE -- TODO: Decide if we need foreign key constraints
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Restriction on collection name
-- Record id length is only stored in the HSNW index and metadata segment.
-- We probably still need to restrict the length of record id. More generous than UUID.
-- Needs to store the epoch in the table.

-- We need to add database and potentially tenant id to the record id.
