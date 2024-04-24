DECLARE tables_to_delete ARRAY<STRING>;

-- Step 1: Retrieve Table Names
SET tables_to_delete = (
  SELECT ARRAY_AGG(table_id)
  FROM `tr-ga4.analytics_314488492.__TABLES_SUMMARY__`
  WHERE table_id LIKE 'events_%'
    AND (
      (table_id BETWEEN 'events_20240401' AND 'events_20240416')
      OR (table_id BETWEEN 'intraday_20240401' AND 'intraday_20240416')
    )
);

-- Step 2: Check the Values in tables_to_delete
SELECT * FROM UNNEST(tables_to_delete);


-- Step 3: Execute DELETE Statements with JOIN
FOR table_name IN (SELECT * FROM UNNEST(tables_to_delete) AS tbl_name) DO
  EXECUTE IMMEDIATE FORMAT(
    """
    DELETE FROM `analytics_314488492.%s`
    WHERE
      user_pseudo_id IN (
        SELECT candidates_user.user_pseudo_id
        FROM
          `tr-ga4.analytics_314488492.session_deletion_candidates` AS candidates_user
      )
      AND
        (SELECT
          MAX(value.int_value)
        FROM
          UNNEST(`analytics_314488492.%s`.event_params)
        WHERE
          key = 'ga_session_id'
          LIMIT 1
      ) = (
        SELECT
          candidates_ga.ga_session_id
        FROM
          `tr-ga4.analytics_314488492.session_deletion_candidates` AS candidates_ga
        LIMIT 1  -- Ensure single result
      )
    """,
    table_name.tbl_name,
    table_name.tbl_name
  );
END FOR;