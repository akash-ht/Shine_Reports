#Daily Allocation of Allocable Items
select 
	h.username as Candidate_Email,
	d.order_id as Order_Id,
	f.name as Item_Category,
	d.name as Item_Name,
	i.name as Item_level,
	IF(j.name is null,'Regular Delivery',j.name) as Delivery_Type,
	IF(b.group_id=1,'Vendor','Writer') as Destination,
	c.username as Detination_Name
from shinecp.cart_orderitemoperation as a
left join shinecp.auth_user_groups as b
on a.assigned_to_id=b.user_id
left join shinecp.auth_user as c
on a.assigned_to_id=c.id
left join shinecp.cart_orderitem as d
on a.order_item_id=d.id
left join shinecp.cart_product as e
on d.product_id=e.id
left join shinecp.cart_category as f
on e.category_id=f.id
left join shinecp.cart_order as g
on d.order_id=g.id
left join shinecp.auth_user as h
on g.candidate_id=h.id
left join shinecp.cart_productvariation as i
on d.variation_id=i.id
left join (select order_id,name from shinecp.cart_orderitem where name in ('Super Express Delivery','Express Delivery')) as j
on d.order_id=j.order_id
where date(a.added_on)=date_add(curdate(), interval -1 day)
and operation_type=8
and e.is_allocable=1
