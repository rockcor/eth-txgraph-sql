SELECT 
    LEAST(from_address, to_address) AS address1,
    GREATEST(from_address, to_address) AS address2,
    SUM(value/POWER(10,18)) AS total_value,  
    AVG(gas_price/POWER(10,18)) as avg_gas_price,
    COUNT(*) AS transactions_count,
    MIN(block_timestamp) AS earliest_timestamp
FROM `bigquery-public-data.crypto_ethereum.transactions`
JOIN
    `western-diorama-415812.address_5k.target-feature-label` l
  ON
    from_address = l.address OR to_address = l.address
WHERE 
    receipt_status=1
    AND block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
GROUP BY
    LEAST(from_address, to_address),
    GREATEST(from_address, to_address)---这里加不加聚合函数居然只差六条边！
HAVING 
    total_value>=1