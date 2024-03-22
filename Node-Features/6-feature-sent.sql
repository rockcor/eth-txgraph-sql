WITH sent_transactions AS (
  SELECT
    t.from_address AS address,
    SUM(t.value) AS total_sent,
    AVG(t.value) AS avg_value_sent,
    MAX(t.value) AS max_value_sent,
    MIN(t.value) AS min_value_sent,
    STDDEV(t.value) AS std_value_sent
  FROM
    `bigquery-public-data.crypto_ethereum.transactions` t
    JOIN
    `western-diorama-415812.address.all-nodes` n ON t.from_address = n.address
  WHERE
    t.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND t.receipt_status = 1
    AND t.value > 0
  GROUP BY t.from_address
)

SELECT
  n.address,
  COALESCE(st.total_sent, 0) AS total_sent,
  COALESCE(st.avg_value_sent, 0) AS avg_value_sent,
  COALESCE(st.max_value_sent, 0) AS max_value_sent,
  COALESCE(st.min_value_sent, 0) AS min_value_sent,
  COALESCE(st.std_value_sent, 0) AS std_value_sent
FROM `western-diorama-415812.address.all-nodes` n
LEFT JOIN sent_transactions st ON n.address = st.address
