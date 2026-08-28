-- Cross-endpoint reconciliation: /daily-visits vs summed /ga-sessions-data.
--
-- Mirrors ga_pipeline/quality.py::check_reconciliation so the ad-hoc answer and
-- the pipeline's own gate agree: session visits are COALESCE(SUM(totals_visits),
-- COUNT(*)), the tolerance is 5%, and only a fully missing side is an error.
--
-- Both sides are filtered on their partition columns (visit_date / session_date)
-- so this prunes partitions instead of scanning the table.
--
-- Substitute the project/dataset, or run through: envsubst < ga_pipeline/sql/reconciliation.sql

DECLARE start_date DATE DEFAULT '2016-08-01';
DECLARE end_date   DATE DEFAULT '2016-08-07';

WITH daily AS (
  SELECT visit_date AS d, visits AS daily_visits
  FROM `${BQ_PROJECT}.${BQ_DATASET}.daily_visits`
  WHERE visit_date BETWEEN start_date AND end_date
),
sessions AS (
  SELECT
    session_date                              AS d,
    COALESCE(SUM(totals_visits), COUNT(*))    AS session_visits,
    COUNT(*)                                  AS session_rows,
    COUNT(DISTINCT full_visitor_id)           AS visitors
  FROM `${BQ_PROJECT}.${BQ_DATASET}.ga_sessions_flat`
  WHERE session_date BETWEEN start_date AND end_date
  GROUP BY d
)
SELECT
  COALESCE(daily.d, sessions.d)                        AS date,
  daily.daily_visits,
  sessions.session_visits,
  sessions.session_rows,
  sessions.visitors,
  sessions.session_visits - daily.daily_visits         AS diff,
  ROUND(SAFE_DIVIDE(sessions.session_visits - daily.daily_visits,
                    daily.daily_visits) * 100, 1)      AS diff_pct,
  CASE
    WHEN daily.daily_visits IS NULL                  THEN 'ERROR: sessions loaded, no daily_visits row'
    WHEN sessions.session_visits IS NULL             THEN 'ERROR: daily_visits row, no sessions loaded'
    WHEN ABS(SAFE_DIVIDE(sessions.session_visits - daily.daily_visits,
                         daily.daily_visits)) <= 0.05 THEN 'ok (within 5%)'
    ELSE 'warn: endpoints disagree (expected; see docs/01-api-exploration.md)'
  END                                                  AS status
FROM daily
FULL OUTER JOIN sessions USING (d)
ORDER BY date;
