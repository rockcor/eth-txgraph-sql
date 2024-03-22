WITH transactions AS (
  SELECT
    t.from_address,
    t.to_address,
    t.value,
    t.block_timestamp
  FROM
    `bigquery-public-data.crypto_ethereum.transactions` t
    JOIN
    `western-diorama-415812.address.all-nodes` n ON t.to_address = n.address
  WHERE
    t.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND t.receipt_status = 1
    AND t.value > 0
),

num_received_single_neighbor AS (
  SELECT
    to_address AS address,
    COUNT(DISTINCT from_address) AS num_received_single_neighbor
  FROM transactions
  GROUP BY to_address
)

SELECT
  n.address,
  COALESCE(nr.num_received_single_neighbor, 0) AS num_received_single_neighbor
FROM `western-diorama-415812.address.all-nodes` n
LEFT JOIN num_received_single_neighbor nr ON n.address = nr.address

