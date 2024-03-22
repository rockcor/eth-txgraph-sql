SELECT
  CASE
    WHEN a1.address IS NULL THEN e.address2
  ELSE
  e.address1
END
  AS target_address,
  CASE
    WHEN a1.address IS NULL THEN e.address1
  ELSE
  e.address2
END
  AS aff_address,
  total_value,
  transactions_count,
  avg_gas_price
FROM
  `western-diorama-415812.address_5k.1-hop-sorted-edges-with-feature` e
LEFT JOIN
  `western-diorama-415812.address_5k.target-with-feature-label` a1
ON
  e.address1 = a1.address
LEFT JOIN
  `western-diorama-415812.address_5k.target-with-feature-label` a2
ON
  e.address2 = a2.address
WHERE
  (a1.address IS NULL
    AND a2.address IS NOT NULL)
  OR (a1.address IS NOT NULL
    AND a2.address IS NULL)