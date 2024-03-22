SELECT 
    LEAST(T.from_address, T.to_address) AS address1,
    GREATEST(T.from_address, T.to_address) AS address2,
    SUM(T.value/POWER(10,18)) AS total_value,
    AVG(T.gas_price/POWER(10,18)) as avg_gas_price,
    COUNT(*) AS transactions_count,
    MIN(T.block_timestamp) AS earliest_timestamp
FROM `bigquery-public-data.crypto_ethereum.transactions` T
JOIN `western-diorama-415812.address_5k.target-feature-label` L1 ON T.from_address = L1.address
JOIN `western-diorama-415812.address_5k.target-feature-label` L2 ON T.to_address = L2.address
WHERE 
    T.receipt_status=1
    AND T.block_timestamp BETWEEN '2018-01-01' AND '2020-01-01'
GROUP BY
    LEAST(T.from_address, T.to_address),
    GREATEST(T.from_address, T.to_address)