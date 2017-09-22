select au.username as CandidateEmail,
	coi.order_id as OrderId,
	IF(dt.name is null,'Regular Delivery',dt.name) as Delivery_Type,
	coi.id as ItemId,if(cc.name is null,cp1.name,cc.name) as ItemCategory,cpv.name as ItemLevel,coi.name as ItemName,
	if(ad.AllocationDate is null,'Stand Alone Service',ad.AllocationDate) as AllocationDate,
	date(coi.added_on) as AddedOnDate,
	date(coi.oio_added_on) as ClosingDate
from shinecp.cart_orderitem as coi
left join shinecp.cart_order as co
on coi.order_id=co.id
left join shinecp.cart_product as cp
on coi.product_id=cp.id
left join shinecp.cart_category as cc
on cp.category_id=cc.id
left join shinecp.cart_productvariation as cpv
on coi.variation_id=cpv.id
left join shinecp.cart_product as cp1
on cp.parent_id=cp1.id
left join shinecp.auth_user as au
on co.candidate_id=au.id
left join 
(
	select order_id,name 
	from shinecp.cart_orderitem 
	where name in ('Super Express Delivery','Express Delivery')) 
as dt
on coi.order_id=dt.order_id
left join 
(
select 
	a.order_id as OrderId,
	date(a.added_on) as AllocationDate
	
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
	select order_id,name 
	from shinecp.cart_orderitem 
	where name in ('Super Express Delivery','Express Delivery')) 
as dt
on a.order_id=dt.order_id
where a.oio_operation_type=4
and date(a.oio_added_on)>'2013-01-01' and date(a.oio_added_on)<=date_add(curdate(), interval -1 day)
and E.Vendor is not null
and au.username<>E.Vendor
and cp.is_allocable=1
#order by 4 
) as ad
on coi.order_id=ad.OrderId
where coi.oio_operation_type in (9,10,11,12)
and date(coi.oio_added_on)=date_add(curdate(), interval -1 day)
group by 1,2,3,4,5,6,7,8,9
order by 9
