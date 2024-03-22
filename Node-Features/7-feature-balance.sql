WITH all_nodes AS (
  SELECT address
  FROM `western-diorama-415812.address.all-nodes`
),

initial_balances AS (
  SELECT
    n.address,
    COALESCE(SUM(t.value), 0) AS starting_balance
  FROM all_nodes n
  LEFT JOIN (
    SELECT
      to_address AS address,
      value
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp < '2018-01-01'
      AND value > 0
      AND receipt_status = 1
    
    UNION ALL
    
    SELECT
      from_address AS address,
      -value AS value
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp < '2018-01-01'
      AND value > 0
      AND receipt_status = 1
  ) t ON n.address = t.address
  GROUP BY n.address
),

balance_changes AS (
  SELECT
    address,
    SUM(received) - SUM(spent) AS balance_change
  FROM (
    SELECT
      to_address AS address,
      value AS received,
      0 AS spent
    FROM
      `bigquery-public-data.crypto_ethereum.transactions`
    WHERE
      block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
      AND value > 0
      AND receipt_status = 1
      AND to_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)

    UNION ALL

    SELECT
      from_address AS address,
      0 AS received,
      value AS spent
    FROM
      `bigquery-public-data.crypto_ethereum.transactions`
    WHERE
      block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
      AND value > 0
      AND receipt_status = 1
      AND from_address IN (SELECT address FROM `western-diorama-415812.address.all-nodes`)
  )
  GROUP BY address
)

SELECT
  n.address,
  COALESCE(ib.starting_balance, 0) AS starting_balance,
  COALESCE(ib.starting_balance, 0) + COALESCE(bc.balance_change, 0) AS final_balance
FROM all_nodes n
LEFT JOIN initial_balances ib ON n.address = ib.address
LEFT JOIN balance_changes bc ON n.address = bc.address
