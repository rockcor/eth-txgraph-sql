SELECT DISTINCT br_node
FROM (
  SELECT br1_first AS br_node FROM `western-diorama-415812.address.2-hop-br` 
  UNION DISTINCT
  SELECT br1_second FROM `western-diorama-415812.address.2-hop-br` 
) AS distinct_br_nodes