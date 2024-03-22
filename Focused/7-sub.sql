SELECT DISTINCT t.aff_address AS address
FROM `western-diorama-415812.address_5k.1-hop-unsorted-edges-without-dtx` AS t
LEFT JOIN `western-diorama-415812.address_5k.br1` a ON t.aff_address = a.address
LEFT JOIN `western-diorama-415812.address_5k.br2` b ON t.aff_address = b.address
WHERE a.address IS NULL AND b.address IS NULL