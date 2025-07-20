# queries.py

def drop_tables_sql():
    return '''
    DROP TABLE IF EXISTS delta;
    DROP TABLE IF EXISTS scd;
    '''

def create_delta_sql():
    return '''
    CREATE OR REPLACE TABLE delta AS (
      SELECT 1 AS id, 190 AS val, TIMESTAMP '2022-06-15' AS fact_ts UNION ALL
      SELECT 1, 180, TIMESTAMP '2022-06-01' UNION ALL
      SELECT 1, 18, TIMESTAMP '2022-06-01' UNION ALL
      SELECT 1, 176, TIMESTAMP '2022-05-30' UNION ALL
      SELECT 1, 174, TIMESTAMP '2022-05-30' UNION ALL
      SELECT 1, 172, TIMESTAMP '2022-05-28' UNION ALL
      SELECT 1, 16, TIMESTAMP '2022-05-24' UNION ALL
      SELECT 1, 150, TIMESTAMP '2022-05-20' UNION ALL
      SELECT 1, 15, TIMESTAMP '2022-05-15' UNION ALL
      SELECT 1, 145, TIMESTAMP '2022-05-13' UNION ALL
      SELECT 1, 140, TIMESTAMP '2022-05-01' UNION ALL
      SELECT 1, 120, TIMESTAMP '2022-04-01' UNION ALL
      SELECT 1, 12, TIMESTAMP '2022-04-01' UNION ALL
      SELECT 1, 100, TIMESTAMP '2022-03-01'
    );
    '''

def create_scd_initial_sql():
    return '''
    CREATE OR REPLACE TABLE scd AS (
      SELECT 1 AS id, 18 AS val, DATE '2022-06-01' AS effective_from, DATE '9999-12-31' AS effective_to, TIMESTAMP '2023-01-01' AS snapshot_update_ts UNION ALL
      SELECT 1, 17, DATE '2022-05-26', DATE '2022-05-31', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 16, DATE '2022-05-24', DATE '2022-05-25', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 15, DATE '2022-05-15', DATE '2022-05-23', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 14, DATE '2022-05-01', DATE '2022-05-14', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 13, DATE '2022-04-15', DATE '2022-04-30', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 12, DATE '2022-04-01', DATE '2022-04-14', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 1, 11, DATE '2022-03-15', DATE '2022-03-31', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 2, 22, DATE '2022-01-15', DATE '9999-12-31', TIMESTAMP '2023-01-01' UNION ALL
      SELECT 3, 33, DATE '2022-01-15', DATE '9999-12-31', TIMESTAMP '2023-01-01'
    );
    '''

def insert_history_1_sql():
    return '''
    -- client 1 history fulfilling
    INSERT INTO scd
    SELECT 1, RANDOM()*10000+1, v::TIMESTAMP,
           v::TIMESTAMP + INTERVAL '106 minutes' - INTERVAL '1 second',
           TIMESTAMP '2023-11-11 23:11:11'
    FROM GENERATE_SERIES(
      TIMESTAMP '2022-03-15' - INTERVAL '106 minutes',
      TIMESTAMP '1-1-1',
      INTERVAL '-106 minutes'
    ) AS t(v);
    '''

def insert_history_23_sql():
    return '''
    -- clients 2 and 3 history fulfilling
    INSERT INTO scd
    SELECT 2, RANDOM()*10000+1, v::TIMESTAMP,
           v::TIMESTAMP + INTERVAL '106 minutes' - INTERVAL '1 second',
           TIMESTAMP '2023-11-11 23:11:12'
    FROM GENERATE_SERIES(
      TIMESTAMP '2022-01-15' - INTERVAL '106 minutes',
      TIMESTAMP '1-1-1',
      INTERVAL '-106 minutes'
    ) AS t(v);

    INSERT INTO scd
    SELECT 3, RANDOM()*10000+1, v::TIMESTAMP,
           v::TIMESTAMP + INTERVAL '106 minutes' - INTERVAL '1 second',
           TIMESTAMP '2023-11-11 23:11:13'
    FROM GENERATE_SERIES(
      TIMESTAMP '2022-01-15' - INTERVAL '106 minutes',
      TIMESTAMP '1-1-1',
      INTERVAL '-106 minutes'
    ) AS t(v);
    '''

def merge_option_1_query():
    return '''
    -- SQL 1-1, WINDOWING BY ID, LEFT JOIN - my implementation
    -- explain analyze
    -- create or replace table scd_final as 

    SELECT COUNT(1) FROM (
    WITH 
      delta_dedup AS (
        SELECT *
        FROM delta
        WINDOW w AS (PARTITION BY id, fact_ts)
        QUALIFY ROW_NUMBER() OVER w = 1
      ),
      affected_ids AS (
        SELECT DISTINCT id FROM delta
      ),
      combined_scd_delta_dedup AS (
        SELECT * FROM scd
        UNION ALL    
        SELECT
            d.id,
            d.val,
            d.fact_ts AS effective_from,
            NULL::DATE AS effective_to,
            strftime(current_timestamp, '%Y-%m-%d %H:%M:%S') AS snapshot_update_ts
        FROM delta_dedup d
        WHERE NOT EXISTS (
            SELECT 1
            FROM scd s
            WHERE s.id = d.id
              AND s.effective_from = d.fact_ts
        )
      ) 

    SELECT 
        c.id,
        c.effective_from,
        CASE
            WHEN a.id IS NOT NULL THEN LEAD(c.effective_from,1 ,DATE '9999-12-31') OVER win_id - INTERVAL '1 day'
            ELSE c.effective_to
        END AS effective_to,
        c.snapshot_update_ts
    FROM combined_scd_delta_dedup c
    LEFT JOIN affected_ids a ON c.id = a.id
    WINDOW win_id AS (PARTITION BY c.id ORDER BY c.effective_from)
    ORDER BY c.id, c.effective_from
    );
    '''

def merge_option_2_query():
    return '''
    -- SCD: option 1-2. full recalc + dedupe delta only. NOT AN OPTION IN GENERAL

    -- explain analyze
    -- create or replace table scd as
    SELECT COUNT(1) FROM (
      WITH combine_dedupe AS (
        SELECT *
        FROM delta
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY sha1(to_json((id, fact_ts)))
          ORDER BY fact_ts
        ) = 1
    
        UNION DISTINCT
    
        SELECT * EXCLUDE (effective_to, snapshot_update_ts)
        FROM scd
      )
    
      SELECT
        id,
        val,
        fact_ts AS effective_from,
        IFNULL(
          LEAD(fact_ts) OVER (
            PARTITION BY sha1(to_json((id)))
            ORDER BY fact_ts
          ) - INTERVAL 1 SECOND,
          DATE '9999-12-31 23:59:59'
        ) AS effective_to,
        CURRENT_TIMESTAMP AS snapshot_update_ts
      FROM combine_dedupe
      -- ORDER BY 1, 3 DESC
    );
    -- create scd
    '''
def merge_option_3_query():
    return '''
        with
    range as (
      select id, min(fact_ts) min_ts, max(fact_ts) max_ts
      from delta group by all
    )
    ,
    combine as (
      select * from delta
      union by name
      select s.* exclude (snapshot_update_ts) rename (effective_from as fact_ts, effective_to as old_effective_to)
      from scd s
      join range r on s.id = r.id and s.effective_to >= r.min_ts
      qualify row_number() over(partition by s.id order by if(s.effective_from > r.max_ts, 1, 0) desc, s.effective_from) = 1
        or s.effective_from <= r.max_ts
    )
    ,
    dedupe as (
      select * from combine
      qualify row_number() over (partition by sha1(to_json((
        id, fact_ts
      ))) order by fact_ts, old_effective_to) = 1
    )
    
    select * exclude(old_effective_to) rename(fact_ts as effective_from),
      ifnull(lead(fact_ts) over (partition by id order by fact_ts) - interval 1 second, timestamp'9999-12-31 23:59:59') as effective_to,
      current_timestamp as snapshot_update_ts
    from dedupe
    qualify effective_to <= ifnull(old_effective_to, timestamp'9999-12-31 23:59:59')
    order by 1, 3 desc
    '''
