-- 合并二阶桥节点效果不好，直接删除
WITH
  all_nodes AS (
  SELECT
    address,
    starting_balance_eth AS starting_balance_eth_avg,
    final_balance_eth AS final_balance_eth_avg,
    final_balance_eth - starting_balance_eth AS diff_balance_eth_avg,
    -- Assuming you want the difference AS the "average"; adjustIF
  total_received_eth AS total_received_eth_avg,
    max_value_received_eth AS max_value_received_eth_avg,
    min_value_received_eth AS min_value_received_eth_avg,
    avg_value_received_eth AS avg_value_received_eth_avg,
    std_value_received_eth AS std_value_received_eth_avg,
    total_sent_eth AS total_sent_eth_avg,
    max_value_sent_eth AS max_value_sent_eth_avg,
    std_value_sent_eth AS std_value_sent_eth_avg,
    avg_value_sent_eth AS avg_value_sent_eth_avg,
    min_value_sent_eth AS min_value_sent_eth_avg,
    avg_single_neighbor_value_eth AS avg_single_neighbor_value_eth_avg,
    avg_single_neighbor_count AS f0_avg,
    -- Assuming avg_single_neighbor_count IS intended TO be f0; adjust
max_single_neighbor_value_eth AS max_single_neighbor_value_eth_avg,
    max_single_neighbor_count AS f1_avg,
    -- Assuming max_single_neighbor_count IS intended TO be f1; adjust
num_received_single_neighbor AS f2_avg,
    -- Assuming num_received_single_neighbor IS intended TO be f2; adjust
num_sent_single_neighbor AS f3_avg,
    -- Assuming num_sent_single_neighbor IS intended TO be f3; adjust
diff_rs_neighbor_count AS diff_rs_neighbor_count_avg,
    std_dev_received AS f4_avg,
    -- Assuming std_dev_received IS intended TO be f4; adjust
std_dev_sent AS f5_avg,
    -- Assuming std_dev_sent IS intended TO be f5; adjust
 lifecycle_min AS f6_avg,
    -- Assuming lifecycle_min IS intended TO be f6; adjust
 avg_min_between_sent_tnx AS f7_avg,
    -- Assuming avg_min_between_sent_tnx IS intended TO be f7; adjust
avg_min_between_sent_value_eth AS avg_min_between_sent_value_eth_avg,
    avg_min_between_received_tnx AS f8_avg,
    -- Assuming avg_min_between_received_tnx IS intended TO be f8; adjust
avg_min_between_received_value_eth AS avg_min_between_received_value_eth_avg,
    CASE
      WHEN if_sc THEN 1
    ELSE
    0
  END
    AS if_sc_avg,
    CASE
      WHEN if_token THEN 1
    ELSE
    0
  END
    AS if_token_avg,
  FROM
    `western-diorama-415812.address_5k.target-feature-label`atn
  LEFT JOIN
    `western-diorama-415812.coarse.target-merge-with-aff` tma
  ON
    atn.address = tma.target_address
  WHERE
    tma.target_address IS NULL
  UNION ALL
  SELECT
    target_address AS address,
    starting_balance_eth_avg,
    final_balance_eth_avg,
    diff_balance_eth_avg,
    total_received_eth_avg,
    max_value_received_eth_avg,
    min_value_received_eth_avg,
    avg_value_received_eth_avg,
    std_value_received_eth_avg,
    total_sent_eth_avg,
    max_value_sent_eth_avg,
    avg_value_sent_eth_avg,
    std_value_sent_eth_avg,
    min_value_sent_eth_avg,
    f0_avg,
    f1_avg,
    f2_avg,
    f3_avg,
    f4_avg,
    f5_avg,
    f6_avg,
    f7_avg,
    f8_avg,
    max_single_neighbor_value_eth_avg,
    avg_single_neighbor_value_eth_avg,
    diff_rs_neighbor_count_avg,
    avg_min_between_sent_value_eth_avg,
    avg_min_between_received_value_eth_avg,
    if_sc_avg,
    if_token_avg,
  FROM
    `western-diorama-415812.coarse.target-merge-with-aff`
  UNION ALL
  SELECT
    CONCAT(target_address1, '_', target_address2) AS address,
    -- 用于唯一标识桥节点 
    starting_balance_eth_avg,
    final_balance_eth_avg,
    diff_balance_eth_avg,
    total_received_eth_avg,
    max_value_received_eth_avg,
    min_value_received_eth_avg,
    avg_value_received_eth_avg,
    std_value_received_eth_avg,
    total_sent_eth_avg,
    max_value_sent_eth_avg,
    avg_value_sent_eth_avg,
    std_value_sent_eth_avg,
    min_value_sent_eth_avg,
    f0_avg,
    f1_avg,
    f2_avg,
    f3_avg,
    f4_avg,
    f5_avg,
    f6_avg,
    f7_avg,
    f8_avg,
    max_single_neighbor_value_eth_avg,
    avg_single_neighbor_value_eth_avg,
    diff_rs_neighbor_count_avg,
    avg_min_between_sent_value_eth_avg,
    avg_min_between_received_value_eth_avg,
    if_sc_avg,
    if_token_avg
  FROM
    `western-diorama-415812.coarse.br1`
  -- UNION ALL
  -- SELECT
  --   CONCAT(target_address1, '_', target_address2,'first') AS address,
  --   -- 用于唯一标识桥节点 
  --   starting_balance_eth_avg,
  --   final_balance_eth_avg,
  --   diff_balance_eth_avg,
  --   total_received_eth_avg,
  --   max_value_received_eth_avg,
  --   min_value_received_eth_avg,
  --   avg_value_received_eth_avg,
  --   std_value_received_eth_avg,
  --   total_sent_eth_avg,
  --   max_value_sent_eth_avg,
  --   avg_value_sent_eth_avg,
  --   std_value_sent_eth_avg,
  --   min_value_sent_eth_avg,
  --   f0_avg,
  --   f1_avg,
  --   f2_avg,
  --   f3_avg,
  --   f4_avg,
  --   f5_avg,
  --   f6_avg,
  --   f7_avg,
  --   f8_avg,
  --   max_single_neighbor_value_eth_avg,
  --   avg_single_neighbor_value_eth_avg,
  --   diff_rs_neighbor_count_avg,
  --   avg_min_between_sent_value_eth_avg,
  --   avg_min_between_received_value_eth_avg,
  --   if_sc_avg,
  --   if_token_avg
  -- FROM
  --   `western-diorama-415812.coarse.br2_first`
  -- UNION ALL
  -- SELECT
  --   CONCAT(target_address1, '_', target_address2,'second') AS address,
  --   -- 用于唯一标识桥节点 
  --   starting_balance_eth_avg,
  --   final_balance_eth_avg,
  --   diff_balance_eth_avg,
  --   total_received_eth_avg,
  --   max_value_received_eth_avg,
  --   min_value_received_eth_avg,
  --   avg_value_received_eth_avg,
  --   std_value_received_eth_avg,
  --   total_sent_eth_avg,
  --   max_value_sent_eth_avg,
  --   avg_value_sent_eth_avg,
  --   std_value_sent_eth_avg,
  --   min_value_sent_eth_avg,
  --   f0_avg,
  --   f1_avg,
  --   f2_avg,
  --   f3_avg,
  --   f4_avg,
  --   f5_avg,
  --   f6_avg,
  --   f7_avg,
  --   f8_avg,
  --   max_single_neighbor_value_eth_avg,
  --   avg_single_neighbor_value_eth_avg,
  --   diff_rs_neighbor_count_avg,
  --   avg_min_between_sent_value_eth_avg,
  --   avg_min_between_received_value_eth_avg,
  --   if_sc_avg,
  --   if_token_avg
  -- FROM
  --   `western-diorama-415812.coarse.br2_second` 
  ),
  indexed_nodes AS (
  SELECT
    *,
    ROW_NUMBER() OVER(ORDER BY address) - 1 AS node_index
  FROM
    all_nodes )
SELECT
  inodes.*,
  -- 将if_illicit的TRUE映射为1，FALSE为0，空值为-1
  CASE
    WHEN tfl.if_illicit IS NULL THEN -1
    WHEN tfl.if_illicit THEN 1
  ELSE
  0
END
  AS if_illicit_label
FROM
  indexed_nodes inodes
LEFT JOIN
  `western-diorama-415812.address_5k.target-feature-label` tfl
ON
  inodes.address = tfl.address;