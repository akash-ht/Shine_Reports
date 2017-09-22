select 	fd.OrderId,
		au.username as CandidateEmail,
		fd.ItemId,
		cc.name as ItemCategory,
		coi.name as ItemName,
		cpv.name as ItemLevel,
		asd.AllocatedTo,
		DATE_FORMAT(ru.ResumeUploadDate,'%e-%M-%Y') as ResumeUploadDate,
		#DATE_FORMAT(ru.ResumeUploadTime,'%h:%i %p') as ResumeUploadTime,
		DATE_FORMAT(asd.AssignedDate,'%e-%M-%Y') as AssignedDate,
		DATE_FORMAT(asd.AssignedTime,'%h:%i %p') as AssignedTime,
		DATE_FORMAT(fd.FirstDraftDate,'%e-%M-%Y') as FirstDraftDate,
		DATE_FORMAT(fd.FirstDraftTime,'%h:%i %p') as FirstDraftTime,
		IF (coi_1.name is null ,'Regular Delivery', coi_1.name) as Delivery_Type 
from
(
	#Fetching information for Draft 1 
	select  T.OrderId,T.ItemId,T.FirstDraftDate,T.FirstDraftTime
		from 
		(
			select co.order_id as OrderId,co.id as ItemId,date(coi.added_on) as FirstDraftDate,time(coi.added_on) as FirstDraftTime
				from shinecp.cart_orderitem as co
				left join shinecp.cart_orderitemoperation as coi
				on co.id=coi.order_item_id
				left join shinecp.cart_product as cp
				on co.product_id=cp.id
				where cp.is_allocable=1 									#Condition for only allocable items
				and coi.operation_type=2 									#Condition for Draft 1 send
		) as T
	where T.ItemId in 	(	select order_item_id 
								from shinecp.cart_orderitemoperation 
								as coi where operation_type=2 					#Condition for Draft1 send
								and date(coi.added_on)=date_add(curdate(), interval -1 day)
						)
) as fd
left join
(
	#Fetching information for Resume Upload
	select  T.OrderId,T.ItemId,T.ResumeUploadDate,T.ResumeUploadTime
		from 
		(
			select co.order_id as OrderId,co.id as ItemId,date(coi.added_on) as ResumeUploadDate,time(coi.added_on) as ResumeUploadTime
				from shinecp.cart_orderitem as co
				left join shinecp.cart_orderitemoperation as coi
				on co.id=coi.order_item_id
				left join shinecp.cart_product as cp
				on co.product_id=cp.id
				where cp.is_allocable=1 									#Condition for only allocable items
				and coi.operation_type=1 									#Condition for Resume Upload
		) as T
	where T.ItemId in 	(	select order_item_id 
								from shinecp.cart_orderitemoperation 
								as coi where operation_type=2 					#Condition for Draft1 send
								and date(coi.added_on)=date_add(curdate(), interval -1 day)
						)
) as ru
on fd.ItemId=ru.ItemId 
left join 
(
select ParentSet.OrderId,ParentSet.ItemId,ParentSet.LatestDate as AssignedDate,ParentSet.LatestTime as AssignedTime,ChildSet.AllocatedTo 
	from 
	(
		select 	set1.OrderId,
				set1.ItemId,
				max(set1.AssignedDate) as LatestDate,
				max(set1.AssignedTime) as LatestTime 
			from 
			(
				#Fetching information for Assigned
				select  T.OrderId,T.ItemId,T.AssignedDate,T.AssignedTime,T.AllocatedTo
					from 
					(
						select co.order_id as OrderId,co.id as ItemId,date(coi.added_on) as AssignedDate,
								time(coi.added_on) as AssignedTime,au.username as 	AllocatedTo
							from shinecp.cart_orderitem as co
							left join shinecp.cart_orderitemoperation as coi
							on co.id=coi.order_item_id
							left join shinecp.cart_product as cp
							on co.product_id=cp.id
							left join shinecp.auth_user as au
							on coi.assigned_to_id=au.id
							where cp.is_allocable=1 									#Condition for only allocable items
							and coi.operation_type=8 									#Condition for Assigned
					) as T
				where T.ItemId in 	(	select order_item_id 
											from shinecp.cart_orderitemoperation 
											as coi where operation_type=2 					#Condition for Draft1 send
											and date(coi.added_on)=date_add(curdate(), interval -1 day)
									)
				group by 1,2,3,4,5
				order by 2
			) as set1
		group by 1,2
	) as ParentSet
left join 
(
select  T.OrderId,T.ItemId,T.AssignedDate,T.AssignedTime,T.AllocatedTo
	from 
	(
		select co.order_id as OrderId,co.id as ItemId,date(coi.added_on) as AssignedDate,time(coi.added_on) as AssignedTime,au.username as 	AllocatedTo
			from shinecp.cart_orderitem as co
			left join shinecp.cart_orderitemoperation as coi
			on co.id=coi.order_item_id
			left join shinecp.cart_product as cp
			on co.product_id=cp.id
			left join shinecp.auth_user as au
			on coi.assigned_to_id=au.id
		where cp.is_allocable=1 									#Condition for only allocable items
		and coi.operation_type=8 									#Condition for Assigned
	) as T
where T.ItemId in 	(	select order_item_id 
							from shinecp.cart_orderitemoperation 
							as coi where operation_type=2 					#Condition for Draft1 send
							and date(coi.added_on)=date_add(curdate(), interval -1 day)
					)
group by 1,2,3,4,5
order by 2
) as ChildSet
on ParentSet.ItemId=ChildSet.ItemId and ParentSet.LatestTime=ChildSet.AssignedTime
) as asd
on fd.ItemId=asd.ItemId
left join shinecp.cart_orderitem as coi
on fd.ItemId=coi.id
left join (select order_id,name from shinecp.cart_orderitem where name in ('Super Express Delivery' , 'Express Delivery')) as coi_1
on coi.order_id = coi_1.order_id
left join shinecp.cart_order as co
on coi.order_id=co.id
left join shinecp.cart_product as cp
on coi.product_id=cp.id
left join shinecp.cart_category as cc
on cp.category_id=cc.id
left join shinecp.cart_productvariation as cpv
on coi.variation_id=cpv.id
left join shinecp.auth_user as au
on co.candidate_id=au.id
group by 1,2,3,4,5,6,7,8,9,10,11
