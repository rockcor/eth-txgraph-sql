-- 直接交易的边
WITH edges_direct AS (
  SELECT
    LEAST(n1.node_index, n2.node_index) AS index1,
    GREATEST(n1.node_index, n2.node_index) AS index2
  FROM `western-diorama-415812.address_5k.dtx` dtx
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` n1 ON dtx.address1 = n1.address
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` n2 ON dtx.address2 = n2.address
),

-- 通过桥节点的边
edges_via_bridge AS (
  SELECT
    n1.node_index AS index1,
    bridge.node_index AS index2
  FROM `western-diorama-415812.coarse.br1` br1
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` n1 ON br1.target_address1 = n1.address
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` bridge ON CONCAT(br1.target_address1, '_', br1.target_address2) = bridge.address
  UNION ALL
  SELECT
    bridge.node_index AS index1,
    n2.node_index AS index2
  FROM `western-diorama-415812.coarse.br1` br1
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` n2 ON br1.target_address2 = n2.address
  JOIN `western-diorama-415812.coarse.target_and _br1_with_label` bridge ON CONCAT(br1.target_address1, '_', br1.target_address2) = bridge.address
),

-- 合并所有边
edges_combined AS (
  SELECT * FROM edges_direct
  UNION ALL
  SELECT * FROM edges_via_bridge
)

SELECT * FROM edges_combined
ORDER BY index1, index2;
