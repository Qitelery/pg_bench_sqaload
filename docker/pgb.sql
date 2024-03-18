select c1_0.group_id,c1_1.id,c1_1.name from groups_relations c1_0 join groups c1_1 on c1_1.id=c1_0.child_id where c1_0.group_id=$1
select o1_0.id,o1_0.date_of_purchase,o1_0.user_login from orders o1_0 where o1_0.id=$1
select p1_0.order_id,p1_0.id,p1_0.amount,p1_0.product_id from purchases p1_0 where p1_0.order_id=$1
select p1_0.id,p1_0.file_path,p1_0.name,p1_0.price from products p1_0 where p1_0.id=$1
select p1_0.group_id,p1_1.id,p1_1.file_path,p1_1.name,p1_1.price from groups_products p1_0 join products p1_1 on p1_1.id=p1_0.product_id where p1_0.group_id=$1
select o1_0.id,o1_0.date_of_purchase,o1_0.user_login from orders o1_0
select o1_0.id,o1_0.date_of_purchase,o1_0.user_login from orders o1_0
SELECT * FROM products p where p.name like CONCAT($2,$1,$3)
SELECT n.nspname = ANY(current_schemas($2)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1
select r1_0.user_id,r1_1.name from users_roles r1_0 join roles r1_1 on r1_1.name=r1_0.role_id where r1_0.user_id=$1
SELECT g.group_id FROM groups_relations g WHERE g.child_id=$1
select g1_0.id,g1_0.name from groups g1_0