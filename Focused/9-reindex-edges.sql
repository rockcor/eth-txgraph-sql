WITH combined_edges AS (
  SELECT 
    address1,
    address2,
    total_value
  FROM (
    SELECT 
      address1,
      address2,
      total_value
    FROM `western-diorama-415812.address_5k.1-hop-sorted-edges-with-feature`
    
    UNION ALL
    
    SELECT 
      address1,
      address2,
      total_value
    FROM `western-diorama-415812.address_5k.dtx`
  )
  
  UNION DISTINCT
  
  SELECT
    br1_first AS address1,
    br1_second AS address2,
    total_value
  FROM `western-diorama-415812.address_5k.br2-with-edge-feature`
),

nodes_with_id AS (
  SELECT 
    address,
    node_index AS id
  FROM `western-diorama-415812.address_5k.all-node-feature`
)

SELECT 
  ce.address1,
  ce.address2,
  n1.id AS id_1,
  n2.id AS id_2,
  ce.total_value
FROM combined_edges ce
JOIN nodes_with_id n1 ON ce.address1 = n1.address
JOIN nodes_with_id n2 ON ce.address2 = n2.address
ORDER BY id_1