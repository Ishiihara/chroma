CREATE DATABASE IF NOT EXISTS chroma_meta CHARACTER SET utf8mb4;

CREATE TABLE chroma_meta.segments (
    id VARCHAR(36) NOT NULL,
    `type` TEXT NOT NULL,
    scope TEXT NOT NULL,
    topic TEXT,
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    collection_id VARCHAR(36) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE -- Decide if we need foreign key constraints
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE chroma_meta.segment_metadata (
    segment_id VARCHAR(36) NOT NULL,
    `key` VARCHAR(256) NOT NULL,
    str_value TEXT,
    int_value INTEGER,
    float_value REAL,
    ts BIGINT UNSIGNED DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (segment_id, `key`),
    FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE CASCADE -- Decide if we need foreign key constraints
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
