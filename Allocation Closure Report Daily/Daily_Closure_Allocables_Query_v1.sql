select 
	au.username as CandidateEmail,
	a.order_id as OrderId,
	IF(dt.name is null,'Regular Delivery',dt.name) as Delivery_Type,
	a.id as ItemId,
	cc.name as ItemCategory,
	cpv.name as ItemLevel,
	a.name as ItemName,
	E.Vendor,
	coip.Allocation_Date as Allocation_Date,
	date(a.oio_added_on) as ClosingDate
from shinecp.cart_orderitem as a
left join 
(
	select A.ItemId,B.assigned_to_id,C.username as Vendor,date(A.LastActionDate) as LastActionDate
	from 
	(
		select order_item_id as ItemId,max(added_on) as LastActionDate
		from shinecp.cart_orderitemoperation as a
		where operation_type=8
		group by 1
	) as A
	left join shinecp.cart_orderitemoperation as B
	on A.ItemId=B.order_item_id and A.LastActionDate=B.added_on
	left join shinecp.auth_user as C
	on B.assigned_to_id=C.id
	where C.username<>'ops'
	group by 1,2,3
) as E
on a.id=E.ItemId
left join shinecp.cart_order as co
on a.order_id=co.id
left join shinecp.auth_user as au
on co.candidate_id=au.id
left join shinecp.cart_product as cp
on a.product_id=cp.id
left join shinecp.cart_category as cc
on cp.category_id=cc.id
left join shinecp.cart_productvariation as cpv
on a.variation_id=cpv.id
left join
(
	select coip.order_item_id,max(date(coip.added_on)) as Allocation_Date
	from shinecp.cart_orderitemoperation as coip
	where coip.operation_type=8
	group by 1
) as coip
on a.id=coip.order_item_id
left join 
(
	select order_id,name 
	from shinecp.cart_orderitem 
	where name in ('Super Express Delivery','Express Delivery')) 
as dt
on a.order_id=dt.order_id
where a.oio_operation_type=4
and date(a.oio_added_on)=date_add(curdate(), interval -1 day)
and E.Vendor is not null
and au.username<>E.Vendor
and cp.is_allocable=1
and a.name not like 'International Resume'
group by 1,2,3,4,5,6,7,8,9,10
