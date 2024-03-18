\SET product random(1002,2000) 
BEGIN;
SET enable_seqscan = off;
SET max_parallel_workers_per_gather = 0;
SELECT product_id FROM purchases WHERE product_id = :product;
END;