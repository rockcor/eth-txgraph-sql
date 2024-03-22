WITH second_order_edges AS (
  SELECT 
    LEAST(t.from_address, t.to_address) AS br1_first,
    GREATEST(t.from_address, t.to_address) AS br1_second,
    SUM(t.value/POWER(10, 18)) AS total_value,
    COUNT(*) AS transactions_count,
    AVG(t.gas_price/POWER(10, 18)) AS avg_gas_price
  FROM 
    `bigquery-public-data.crypto_ethereum.transactions` t
  JOIN 
    `western-diorama-415812.address.1-hop-edges` e1 ON t.from_address = e1.aff_address
  JOIN 
    `western-diorama-415812.address.1-hop-edges` e2 ON t.to_address = e2.aff_address
  WHERE 
    t.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
    AND t.from_address != t.to_address
  GROUP BY 
        LEAST(t.from_address, t.to_address),
    GREATEST(t.from_address, t.to_address)
  HAVING
    total_value>=1
)
SELECT
  e.target_address AS target_address1,
  e2.target_address AS target_address2,
  seo.br1_first,
  seo.br1_second,
  seo.total_value,
  seo.transactions_count,
  seo.avg_gas_price
FROM
  second_order_edges seo
JOIN
  `western-diorama-415812.address.1-hop-edges` e ON seo.br1_first = e.aff_address
JOIN
  `western-diorama-415812.address.1-hop-edges` e2 ON seo.br1_second = e2.aff_address