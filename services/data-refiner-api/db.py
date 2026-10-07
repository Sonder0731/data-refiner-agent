"""PostgreSQL schema initialization."""

import os

import psycopg


DEFAULT_DATABASE_DSN = (
    "host=data-refiner-postgres port=5432 dbname=data_refiner "
    "user=data_refiner password=data_refiner_password"
)

# ponytail: startup-time idempotent DDL is sufficient until migrations need
# ordering or rollback.
TABLE_DDL = (
    """
    CREATE EXTENSION IF NOT EXISTS vector
    """,
    """
    CREATE TABLE IF NOT EXISTS workspace_mapping (
        user_id TEXT PRIMARY KEY,
        user_name TEXT NOT NULL,
        container_name TEXT NOT NULL,
        api_url TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    ALTER TABLE workspace_mapping
    ADD COLUMN IF NOT EXISTS user_name TEXT
    """,
    """
    UPDATE workspace_mapping
    SET user_name = user_id
    WHERE user_name IS NULL
    """,
    """
    ALTER TABLE workspace_mapping
    ALTER COLUMN user_name SET NOT NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS workspace_mapping_user_name_idx
    ON workspace_mapping (user_name)
    """,
    """
    CREATE TABLE IF NOT EXISTS data_metadata (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL,
        category TEXT NOT NULL
            CHECK (category IN ('input', 'test_output', 'output')),
        content TEXT NOT NULL DEFAULT '',
        UNIQUE (request_id, category)
    )
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'data_metadata_category_check'
              AND conrelid = 'data_metadata'::regclass
              AND pg_get_constraintdef(oid) NOT LIKE '%test_output%'
        ) THEN
            ALTER TABLE data_metadata
            DROP CONSTRAINT data_metadata_category_check;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'data_metadata_category_check'
              AND conrelid = 'data_metadata'::regclass
        ) THEN
            ALTER TABLE data_metadata
            ADD CONSTRAINT data_metadata_category_check
            CHECK (category IN ('input', 'test_output', 'output'));
        END IF;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        IF to_regclass('public.data_desc') IS NOT NULL
           AND to_regclass('public.base_dataset') IS NULL THEN
            ALTER TABLE data_desc RENAME TO base_dataset;
        END IF;
    END;
    $$
    """,
    """
    CREATE TABLE IF NOT EXISTS base_dataset (
        id BIGSERIAL PRIMARY KEY,
        source_type TEXT NOT NULL,
        source_identifier TEXT NOT NULL,
        data_name TEXT NOT NULL,
        data_description TEXT NOT NULL,
        embedding VECTOR(384),
        field_descriptions JSONB NOT NULL DEFAULT '{}'::JSONB,
        examples JSONB NOT NULL DEFAULT '[]'::JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (source_type, source_identifier),
        CHECK (jsonb_typeof(field_descriptions) = 'object'),
        CHECK (jsonb_typeof(examples) = 'array')
    )
    """,
    """
    ALTER TABLE base_dataset
    ADD COLUMN IF NOT EXISTS embedding VECTOR(384)
    """,
    """
    CREATE TABLE IF NOT EXISTS operators_evaluation (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL,
        rounds INTEGER NOT NULL CHECK (rounds > 0),
        content TEXT NOT NULL DEFAULT '',
        UNIQUE (request_id, rounds)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS new_operator_build_reference (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL,
        rounds INTEGER NOT NULL CHECK (rounds > 0),
        content TEXT NOT NULL DEFAULT '',
        UNIQUE (request_id, rounds)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL,
        category TEXT NOT NULL CHECK (category IN ('test', 'production')),
        content TEXT NOT NULL DEFAULT '',
        UNIQUE (request_id, category)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS spark_runtime_conifg (
        id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL,
        config TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS new_operator_docs (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL,
        content TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS new_operator_docs_request_id_idx
    ON new_operator_docs (request_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS history (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL UNIQUE,
        content JSONB NOT NULL DEFAULT '[]'::JSONB
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS request_state (
        id BIGSERIAL PRIMARY KEY,
        request_id UUID NOT NULL UNIQUE,
        user_id TEXT NOT NULL,
        user_name TEXT NOT NULL,
        room_id TEXT NOT NULL,
        current_stage TEXT NOT NULL,
        user_request TEXT NOT NULL,
        input_data_metadata_id BIGINT REFERENCES data_metadata (id),
        operators_evaluation_id BIGINT REFERENCES operators_evaluation (id),
        new_operator_build_reference_id BIGINT
            REFERENCES new_operator_build_reference (id),
        new_operator_docs BIGINT[] NOT NULL DEFAULT '{}',
        test_pipeline_id BIGINT REFERENCES pipeline (id),
        pipeline_id BIGINT REFERENCES pipeline (id),
        test_output_data_metadata_id BIGINT REFERENCES data_metadata (id),
        output_data_metadata_id BIGINT REFERENCES data_metadata (id),
        history_id BIGINT REFERENCES history (id),
        active_handoff JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_case (
        id BIGSERIAL PRIMARY KEY,
        original_user_query TEXT NOT NULL,
        belong TEXT NOT NULL CHECK (belong IN ('data-refiner', 'user')),
        user_id TEXT,
        input_data_desc TEXT NOT NULL,
        pipeline TEXT NOT NULL,
        task_summary TEXT NOT NULL,
        processing_steps TEXT NOT NULL,
        output_data_description TEXT NOT NULL,
        spark_runtime_config TEXT NOT NULL,
        create_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        update_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (
            (belong = 'user' AND user_id IS NOT NULL)
            OR (belong = 'data-refiner' AND user_id IS NULL)
        )
    )
    """,
    """
    ALTER TABLE pipeline_case
    DROP COLUMN IF EXISTS rewrite_user_query
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_retrieval_index (
        id BIGSERIAL PRIMARY KEY,
        pipeline_case_id BIGINT NOT NULL
            REFERENCES pipeline_case (id) ON DELETE CASCADE,
        retrieval_text TEXT NOT NULL,
        embedding VECTOR(384) NOT NULL
    )
    """,
)


def init_database() -> None:
    """Create all application tables before the API starts."""
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        for statement in TABLE_DDL:
            connection.execute(statement)
