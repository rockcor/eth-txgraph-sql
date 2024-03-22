WITH lifecycle_stats AS (
  SELECT
    address,
    COALESCE(MAX(block_timestamp), '2020-01-01') AS max_timestamp,
    COALESCE(MIN(block_timestamp), '2018-01-01') AS min_timestamp,
    GREATEST(TIMESTAMP_DIFF(COALESCE(MAX(block_timestamp), TIMESTAMP '2020-01-01'), COALESCE(MIN(block_timestamp), TIMESTAMP '2018-01-01'), MINUTE), 1) AS lifecycle_min
  FROM (
    SELECT from_address AS address, block_timestamp 
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
      AND receipt_status = 1
      AND value > 0
      AND (from_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
           OR to_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`))
    UNION ALL
    SELECT to_address AS address, block_timestamp 
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
      AND receipt_status = 1
      AND value > 0
      AND (from_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
           OR to_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`))
  )
  GROUP BY address
),
from_transactions AS (
  SELECT
    from_address AS address,
    COUNT(*) AS total_sent_transactions,
    SUM(value) AS total_sent_value
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND receipt_status = 1
    AND value > 0
    AND from_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
  GROUP BY from_address
),
from_transaction_features AS (
  SELECT
    ls.address,
    COALESCE(ft.total_sent_transactions, 0) AS total_sent_transactions,
    COALESCE(ft.total_sent_value, 0) AS total_sent_value,
    ls.lifecycle_min,
    COALESCE(ft.total_sent_transactions, 0) / ls.lifecycle_min AS avg_min_between_sent_tnx,
    COALESCE(ft.total_sent_value, 0) / ls.lifecycle_min AS avg_min_between_sent_value
  FROM lifecycle_stats ls
  LEFT JOIN from_transactions ft ON ls.address = ft.address
),
to_transactions AS (
  SELECT
    to_address AS address,
    COUNT(*) AS total_received_transactions,
    SUM(value) AS total_received_value
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND receipt_status = 1
    AND value > 0
    AND to_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
  GROUP BY to_address
),
to_transaction_features AS (
  SELECT
    ls.address,
    COALESCE(tt.total_received_transactions, 0) AS total_received_transactions,
    COALESCE(tt.total_received_value, 0) AS total_received_value,
    ls.lifecycle_min,
    COALESCE(tt.total_received_transactions, 0) / ls.lifecycle_min AS avg_min_between_received_tnx,
    COALESCE(tt.total_received_value, 0) / ls.lifecycle_min AS avg_min_between_received_value
  FROM lifecycle_stats ls
  LEFT JOIN to_transactions tt ON ls.address = tt.address
)
SELECT
  n.address,
  COALESCE(ftf.lifecycle_min, 0) AS lifecycle_min, -- 填充孤立节点的lifecycle_min为空值为0
  COALESCE(ftf.avg_min_between_sent_tnx, 0) AS avg_min_between_sent_tnx,
  COALESCE(ftf.avg_min_between_sent_value, 0) AS avg_min_between_sent_value,
  COALESCE(ttf.avg_min_between_received_tnx, 0) AS avg_min_between_received_tnx,
  COALESCE(ttf.avg_min_between_received_value, 0) AS avg_min_between_received_value
FROM `western-diorama-415812.address.all-nodes` n
LEFT JOIN from_transaction_features ftf ON n.address = ftf.address
LEFT JOIN to_transaction_features ttf ON n.address = ttf.address
