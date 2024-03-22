WITH features AS (
  SELECT
      a.address AS address,
      --balance
      COALESCE(CAST(b.starting_balance / POWER(10, 18) AS FLOAT64), 0) AS starting_balance_eth,
      COALESCE(CAST(b.final_balance / POWER(10, 18) AS FLOAT64), 0) AS final_balance_eth,
      COALESCE(CAST((b.final_balance-b.starting_balance) / POWER(10, 18) AS FLOAT64), 0) AS diff_balance_eth,
      --received
      COALESCE(CAST(frv.total_received / POWER(10, 18) AS FLOAT64), 0) AS total_received_eth,
      COALESCE(CAST(frv.max_value_received / POWER(10, 18) AS FLOAT64), 0) AS max_value_received_eth,
      COALESCE(CAST(frv.min_value_received / POWER(10, 18) AS FLOAT64), 0) AS min_value_received_eth,
      COALESCE(CAST(frv.avg_value_received / POWER(10, 18) AS FLOAT64), 0) AS avg_value_received_eth,
      COALESCE(CAST(frv.std_value_received / POWER(10, 18) AS FLOAT64), 0) AS std_value_received_eth,
      --sent
      COALESCE(CAST(fsv.total_sent / POWER(10, 18) AS FLOAT64), 0) AS total_sent_eth,
      COALESCE(CAST(fsv.max_value_sent / POWER(10, 18) AS FLOAT64), 0) AS max_value_sent_eth,
      COALESCE(CAST(fsv.avg_value_sent / POWER(10, 18) AS FLOAT64), 0) AS avg_value_sent_eth,
      COALESCE(CAST(fsv.std_value_sent / POWER(10, 18) AS FLOAT64), 0) AS std_value_sent_eth,
      COALESCE(CAST(fsv.min_value_sent / POWER(10, 18) AS FLOAT64), 0) AS min_value_sent_eth,
      --single_neighbor_undirect
      COALESCE(CAST(funt.max_single_neighbor_value / POWER(10, 18) AS FLOAT64), 0) AS max_single_neighbor_value_eth,
      COALESCE(funt.avg_single_neighbor_count, 0),
      COALESCE(funt.max_single_neighbor_count, 0),
      COALESCE(CAST(funt.avg_single_neighbor_value / POWER(10, 18) AS FLOAT64), 0) AS avg_single_neighbor_value_eth,
      --single_neighbor_direct_count
      COALESCE(fsc.num_received_single_neighbor, 0),
      COALESCE(fsc.num_sent_single_neighbor, 0),
      COALESCE(fsc.num_received_single_neighbor-fsc.num_sent_single_neighbor, 0) AS diff_rs_neighbor_count,
      COALESCE(fss.std_dev_received, 0),
      COALESCE(fss.std_dev_sent, 0),
      --lifecycle
      COALESCE(flc.lifecycle_min, 0),
      COALESCE(flc.avg_min_between_sent_tnx, 0),
      COALESCE(CAST(flc.avg_min_between_sent_value / POWER(10, 18) AS FLOAT64), 0) AS avg_min_between_sent_value_eth,
      COALESCE(flc.avg_min_between_received_tnx, 0),
      COALESCE(CAST(flc.avg_min_between_received_value / POWER(10, 18) AS FLOAT64), 0) AS avg_min_between_received_value_eth,
      --role
      a.if_sc,
      a.if_token
  FROM `western-diorama-415812.address_5k.all-node-role` a
  LEFT JOIN `western-diorama-415812.address.feature-balance` b ON a.address = b.address
  LEFT JOIN `western-diorama-415812.address.feature-single-neighbor-count` fsc ON a.address = fsc.address
  LEFT JOIN `western-diorama-415812.address.feature-single-neighbor-std` fss ON a.address = fss.address
  LEFT JOIN `western-diorama-415812.address.feature-lifecycle` flc ON a.address = flc.address
  LEFT JOIN `western-diorama-415812.address.feature-received` frv ON a.address = frv.address
  LEFT JOIN `western-diorama-415812.address.feature-sent` fsv ON a.address = fsv.address
  LEFT JOIN `western-diorama-415812.address.feature-undirected-neighbor-tx` funt ON a.address = funt.address
),
indexed_features AS (
  SELECT
      ROW_NUMBER() OVER(ORDER BY address) AS node_index,
      *
  FROM features
)
SELECT * FROM indexed_features