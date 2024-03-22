WITH br_feature AS(
  select *
    FROM
    `western-diorama-415812.address_5k.2-hop-br` br
  JOIN
    `western-diorama-415812.address_5k.all-nodes-feature` anf
    ON br.br1_first=anf.address
),
 avg_features AS (
  SELECT
    LEAST(target_address1,target_address2) AS target_address1,
    GREATEST(target_address1,target_address2) AS target_address2,
    AVG(starting_balance_eth) AS starting_balance_eth_avg,
    AVG(final_balance_eth) AS final_balance_eth_avg,
    AVG(diff_balance_eth) AS diff_balance_eth_avg,
    AVG(total_received_eth) AS total_received_eth_avg,
    AVG(max_value_received_eth) AS max_value_received_eth_avg,
    AVG(min_value_received_eth) AS min_value_received_eth_avg,
    AVG(avg_value_received_eth) AS avg_value_received_eth_avg,
    AVG(std_value_received_eth) AS std_value_received_eth_avg,
    AVG(total_sent_eth) AS total_sent_eth_avg,
    AVG(max_value_sent_eth) AS max_value_sent_eth_avg,
    AVG(avg_value_sent_eth) AS avg_value_sent_eth_avg,
    AVG(std_value_sent_eth) AS std_value_sent_eth_avg,
    AVG(min_value_sent_eth) AS min_value_sent_eth_avg,
    AVG(max_single_neighbor_value_eth) AS max_single_neighbor_value_eth_avg,
    AVG(f0_) AS f0_avg,
    AVG(f1_) AS f1_avg,
    AVG(avg_single_neighbor_value_eth) AS avg_single_neighbor_value_eth_avg,
    AVG(f2_) AS f2_avg,
    AVG(f3_) AS f3_avg,
    AVG(diff_rs_neighbor_count) AS diff_rs_neighbor_count_avg,
    AVG(f4_) AS f4_avg,
    AVG(f5_) AS f5_avg,
    AVG(f6_) AS f6_avg,
    AVG(f7_) AS f7_avg,
    AVG(avg_min_between_sent_value_eth) AS avg_min_between_sent_value_eth_avg,
    AVG(f8_) AS f8_avg,
    AVG(avg_min_between_received_value_eth) AS avg_min_between_received_value_eth_avg,
    AVG(CASE WHEN if_sc THEN 1 ELSE 0 END) AS if_sc_avg,
    AVG(CASE WHEN if_token THEN 1 ELSE 0 END) AS if_token_avg
  FROM
    br_feature br
 GROUP BY
    LEAST(target_address1, target_address2),
    GREATEST(target_address1, target_address2)
)
SELECT * FROM avg_features;
