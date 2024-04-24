
---- PREP STEPS ----
-- step 1
CREATE TABLE `3144_test_22apr2024.session_data_to_delete` AS
SELECT
  events.*  -- Use wildcard to capture all columns for schema
FROM
  `tr-ga4.3144_test_22apr2024.events_*` AS events
WHERE
  1 = 0;  

-- step 2
CREATE TABLE `tr-ga4.3144_test_22apr2024.session_deletion_candidates` AS
SELECT 
  user_pseudo_id,
  (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id'
  ) AS ga_session_id
FROM
  `tr-ga4.3144_test_22apr2024.events_*`
WHERE
  ( _TABLE_SUFFIX BETWEEN '20240401' AND '20240416'
    OR _TABLE_SUFFIX BETWEEN 'intraday_20240401' AND 'intraday_20240416' )
GROUP BY
  user_pseudo_id,
  ga_session_id  
HAVING
  COUNT(*) > 10000;


-- step 3
INSERT INTO `3144_test_22apr2024.session_data_to_delete`
SELECT
  events.*
FROM
  `tr-ga4.3144_test_22apr2024.events_*` AS events
JOIN
  `tr-ga4.3144_test_22apr2024.session_deletion_candidates` as high_count_sessions
ON
  events.user_pseudo_id = high_count_sessions.user_pseudo_id
  AND
  (
    (SELECT 
      MAX(value.int_value) 
    FROM 
      UNNEST(events.event_params) 
    WHERE 
      key = 'ga_session_id'
    ) 
  ) = high_count_sessions.ga_session_id;

-- step 4
-- verify query results before deletion
SELECT
  count(*) as count,
  min(event_date) min_event_date,
  max(event_date) max_event_date,
  user_pseudo_id,
  (
    SELECT
      value.int_value
    FROM
      UNNEST (event_params)
    WHERE
      KEY = 'ga_session_id' )  AS session_id 
FROM
    `tr-ga4.3144_test_22apr2024.session_data_to_delete`
GROUP BY
  user_pseudo_id,
  session_id
HAVING count > 10000
;
