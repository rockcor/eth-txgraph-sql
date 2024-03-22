WITH combined_addresses AS (
  SELECT 
    address,
    TRUE as if_aff,
    FALSE as if_1_hop,
    FALSE as if_2_hop,
  FROM `western-diorama-415812.address_5k.aff`
  
  UNION ALL
  
  SELECT 
    address,
    FALSE as if_aff,
    TRUE as if_1_hop,
    FALSE as if_2_hop,
  FROM `western-diorama-415812.address_5k.br1`
  
  UNION ALL
  
  SELECT 
    address,
    FALSE as if_aff,
    FALSE as if_1_hop,
    TRUE as if_2_hop,
  FROM `western-diorama-415812.address_5k.br2`
  
  UNION ALL
  
  SELECT 
    address,
    FALSE as if_aff,
    FALSE as if_1_hop,
    FALSE as if_2_hop,
  FROM `western-diorama-415812.address_5k.target-with-feature-label`
),
token_mark AS (
  SELECT
    address,
    TRUE as if_token
  FROM `bigquery-public-data.crypto_ethereum.tokens`
  WHERE block_timestamp <= '2020-01-01'
),
sc_mark AS (
  SELECT
    address,
    TRUE as if_sc
  FROM `bigquery-public-data.crypto_ethereum.contracts`
  WHERE block_timestamp <= '2020-01-01'
),
addresses_with_sc AS (
  SELECT
    ca.address,
    IF(sc.address IS NOT NULL, TRUE, FALSE) AS if_sc,
    IF(tk.address IS NOT NULL, TRUE, FALSE) AS if_token,
    FALSE as if_aff,
    FALSE as if_1_hop,
    FALSE as if_2_hop,
  FROM combined_addresses ca
  LEFT JOIN sc_mark sc ON ca.address = sc.address
  LEFT JOIN token_mark tk ON ca.address = tk.address
)

-- 对节点去重并合并重叠的标记
SELECT
  address,
  MAX(if_sc) AS if_sc,
  MAX(if_token) AS if_token,
  MAX(if_aff) AS if_aff,
  MAX(if_1_hop) AS if_1_hop,
  MAX(if_2_hop) AS if_2_hop,
FROM addresses_with_sc
GROUP BY address