-- Ensure canonical index name for scored_at ordering in scoring visitors API.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_mart_visitor_scoring_scored_at'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_scoring_scored_at'
    ) THEN
        EXECUTE 'ALTER INDEX idx_mart_visitor_scoring_scored_at RENAME TO idx_scoring_scored_at';
    END IF;
END
$$;

create index if not exists idx_scoring_scored_at
    on mart_visitor_scoring(scored_at desc);
