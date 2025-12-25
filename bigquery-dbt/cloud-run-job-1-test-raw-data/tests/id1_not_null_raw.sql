-- Test raw data BEFORE transformation
SELECT 
    ID1
FROM {{ source('data_raw', 'raw_data') }}
WHERE ID1 IS NULL  -- Returns rows that FAIL the test