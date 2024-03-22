WITH transactions AS (
  SELECT
    CASE
      WHEN from_address < to_address THEN from_address
      ELSE to_address
    END AS address1,
    CASE
      WHEN from_address < to_address THEN to_address
      ELSE from_address
    END AS address2,
    value,
    block_timestamp
  FROM `bigquery-public-data.crypto_ethereum.transactions` t
  WHERE t.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND t.receipt_status = 1
    AND t.value > 0
    AND (from_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
         OR to_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`))
),

neighbor_transactions_aggregated AS (
  SELECT
    address1,
    address2,
    COUNT(*) AS transaction_count,
    SUM(value) AS total_value
  FROM transactions
  GROUP BY address1, address2
),

neighbor_features AS (
  SELECT
    address,
    MAX(transaction_count) AS max_single_neighbor_count,
    MAX(total_value) AS max_single_neighbor_value
  FROM (
    SELECT address1 AS address, transaction_count, total_value FROM neighbor_transactions_aggregated
    UNION ALL
    SELECT address2 AS address, transaction_count, total_value FROM neighbor_transactions_aggregated
  )
  GROUP BY address
),

average_features AS (
  SELECT
    address,
    AVG(transaction_count) AS avg_single_neighbor_count,
    AVG(total_value) AS avg_single_neighbor_value
  FROM (
    SELECT address1 AS address, transaction_count, total_value FROM neighbor_transactions_aggregated
    UNION ALL
    SELECT address2 AS address, transaction_count, total_value FROM neighbor_transactions_aggregated
  )
  GROUP BY address
),

all_nodes_features AS (
  SELECT
    n.address,
    COALESCE(nf.max_single_neighbor_count, 0) AS max_single_neighbor_count,
    COALESCE(nf.max_single_neighbor_value, 0) AS max_single_neighbor_value,
    COALESCE(af.avg_single_neighbor_count, 0) AS avg_single_neighbor_count,
    COALESCE(af.avg_single_neighbor_value, 0) AS avg_single_neighbor_value
  FROM `western-diorama-415812.address.all-nodes` n
  LEFT JOIN neighbor_features nf ON n.address = nf.address
  LEFT JOIN average_features af ON n.address = af.address
)

SELECT * FROM all_nodes_features
