select a.order_id as OrderId,a.id as ItemId,
	g.name as ItemCategory,
	f.name as ItemProduct,
	a.name as ItemName,
	IF(a.oio_operation_type=2,'Draft 1 Send',IF(a.oio_operation_type=3,'Draft 2 Send',IF(a.oio_operation_type=5,'Admin Rejected',IF(a.oio_operation_type=6,'Candidate Rejected',IF(a.oio_operation_type=7,'Admin Approval Pending',IF(a.oio_operation_type=0,'Not Uploaded',IF(a.oio_operation_type=1,'Resume Uploaded','Assigned'))))))) as OerationType,
	#a.oio_assigned_by_id as AssignedById,
	#a.oio_assigned_to_id as AssignedToId,	
	#IF(c.group_id=1,'Vendor','Writer') as AssignedTo,
	e.username as AssigneedByName,
	d.username as AssigneedToName,
	date(a.oio_added_on) as AssignedDate
from shinecp.cart_orderitem as a
left join shinecp.cart_order as b
on a.order_id=b.id
left join shinecp.auth_user_groups as c
on a.oio_assigned_to_id=c.user_id
left join shinecp.auth_user as d
on a.oio_assigned_to_id=d.id
left join shinecp.auth_user as e
on a.oio_assigned_by_id=e.id
left join shinecp.cart_product as f
on a.product_id=f.id
left join shinecp.cart_category as g
on f.category_id=g.id
where a.oio_operation_type not in (0,1,4,9,10,11,12)
and b.status=2
and f.is_allocable=1
order by 2

