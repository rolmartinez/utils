DECLARE tables_to_delete ARRAY<STRING>;

-- Step 1: Retrieve Table Names
SET tables_to_delete = (
  SELECT ARRAY_AGG(table_id)
  FROM `tr-ga4.3144_test_22apr2024.__TABLES_SUMMARY__`
  WHERE table_id LIKE 'events_%'
    AND (
      (table_id BETWEEN 'events_20240401' AND 'events_20240416')
      OR (table_id BETWEEN 'intraday_20240401' AND 'intraday_20240416')
    )
);

-- Step 2: Check the Values in tables_to_delete
SELECT * FROM UNNEST(tables_to_delete);

-- Step 3: Delete Rows Based on Conditions
FOR table_name IN (SELECT * FROM UNNEST(tables_to_delete) tbl_name) DO
  EXECUTE IMMEDIATE FORMAT(
    """
    DELETE FROM `3144_test_22apr2024.%s`
    WHERE user_pseudo_id IN (
      SELECT user_pseudo_id
      FROM `tr-ga4.3144_test_22apr2024.session_deletion_candidates`
    )
    """,
    table_name.tbl_name
  );
END FOR;
