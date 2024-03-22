SELECT

  LEAST(e1.target_address, e2.target_address) AS target_address1,
  GREATEST(e1.target_address, e2.target_address) AS target_address2,
  e1.aff_address AS br1
FROM
  `western-diorama-415812.address.1-hop-edges` e1
JOIN
  `western-diorama-415812.address.1-hop-edges` e2
ON
  e1.aff_address = e2.aff_address  -- 通过同一个背景节点连接
  AND e1.target_address < e2.target_address  -- 确保两个target_address不同且无重复的组合
