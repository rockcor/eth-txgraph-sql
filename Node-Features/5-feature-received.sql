WITH received_transactions AS (
  SELECT
    t.to_address AS address,
    SUM(t.value) AS total_received,
    AVG(t.value) AS avg_value_received,
    MAX(t.value) AS max_value_received,
    MIN(t.value) AS min_value_received,
    STDDEV(t.value) AS std_value_received
  FROM
    `bigquery-public-data.crypto_ethereum.transactions` t
    JOIN
    `western-diorama-415812.address.all-nodes` n ON t.to_address = n.address
  WHERE
    t.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND t.receipt_status = 1
    AND t.value > 0
  GROUP BY t.to_address
)

SELECT
  n.address,
  COALESCE(rt.total_received, 0) AS total_received,
  COALESCE(rt.avg_value_received, 0) AS avg_value_received,
  COALESCE(rt.max_value_received, 0) AS max_value_received,
  COALESCE(rt.min_value_received, 0) AS min_value_received,
  COALESCE(rt.std_value_received, 0) AS std_value_received
FROM `western-diorama-415812.address.all-nodes` n
LEFT JOIN received_transactions rt ON n.address = rt.address
