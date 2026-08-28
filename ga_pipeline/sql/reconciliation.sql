-- Cross-endpoint reconciliation: /daily-visits vs /ga-sessions-data.
--
-- The two endpoints date a session differently:
--   * /ga-sessions-data shards on GA's `date`, the property's LOCAL day. For
--     this property that is UTC-7: every session_date runs 07:00Z to 07:00Z
--     the following day.
--   * /daily-visits counts by UTC date.
-- So a session at 20:00 local on the 1st is 03:00 UTC on the 2nd, and the two
-- sources file it under different dates. Comparing sessions by session_date
-- against daily_visits compares two different days; the gap is the
-- day-over-day change in evening traffic, which ran 0.5-32% over the first
-- week and told you nothing about load correctness.
--
-- This query instead counts sessions by the UTC date of visit_start_time —
-- the same day /daily-visits is counting — so the two sides agree exactly and
-- any difference is a real load problem.
--
-- Sessions belonging to UTC day D live in the D-1 and D partitions, so both
-- are read. Both sides stay filtered on their partition columns, so this
-- prunes partitions instead of scanning the table.
--
-- Mirrors ga_pipeline/quality/checks.py::check_reconciliation.
-- Substitute the project/dataset, or run through:
--   envsubst < ga_pipeline/sql/reconciliation.sql

DECLARE start_date DATE DEFAULT '2016-08-01';
DECLARE end_date   DATE DEFAULT '2016-08-07';

WITH daily AS (
  SELECT visit_date AS d, visits AS daily_visits
  FROM `${BQ_PROJECT}.${BQ_DATASET}.daily_visits`
  WHERE visit_date BETWEEN start_date AND end_date
),
sessions AS (
  SELECT
    DATE(visit_start_time)                    AS d,
    COALESCE(SUM(totals_visits), COUNT(*))    AS session_visits,
    COUNT(*)                                  AS session_rows,
    COUNT(DISTINCT full_visitor_id)           AS visitors
  FROM `${BQ_PROJECT}.${BQ_DATASET}.ga_sessions_flat`
  -- one day either side, because a UTC day draws on two local-day partitions
  WHERE session_date BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY)
                         AND DATE_ADD(end_date, INTERVAL 1 DAY)
    AND DATE(visit_start_time) BETWEEN start_date AND end_date
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
    WHEN daily.daily_visits IS NULL       THEN 'ERROR: sessions loaded, no daily_visits row'
    WHEN sessions.session_visits IS NULL  THEN 'ERROR: daily_visits row, no sessions loaded'
    WHEN sessions.session_visits = daily.daily_visits THEN 'ok (exact)'
    ELSE 'investigate: missed page, partial load, or the adjacent partition is not loaded'
  END                                                  AS status
FROM daily
FULL OUTER JOIN sessions USING (d)
ORDER BY date;
