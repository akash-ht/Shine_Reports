select A.Order_Id,
	A.vendor,
	B.username as Owner_Name,
	A.Order_Date,
	A.Payment_Date,
	A.Sales_Id,
	A.Item_Id,
	A.Item_Category,
	A.Item_Level,
	A.Item_Name,
	A.Candidate_Name,
	A.Candidate_Email,
	A.Candidate_Location,
	A.Candidate_ContactNumber,
	A.GrossPrice_Order,
	A.NetPrice_Order,
	A.EffectiveDiscount_Order,
	A.NetPrice_Item,
	A.EffectivePrice_Item,
	A.Coupon_Code,
	A.Coupon_Discount,
	CASE WHEN A.Payment_mode = '0' THEN 'Not Paid'
		 WHEN A.Payment_mode = '1' THEN 'Cash/Payment.Shine.com'
		 WHEN A.Payment_mode = '2' THEN 'Citrus_Pay'
		 WHEN A.Payment_mode = '3' THEN 'EMI'
		 WHEN A.Payment_mode = '4' THEN 'Cheque or Draft'
		 WHEN A.Payment_mode = '5' THEN 'CC-Avenue'
		 WHEN A.Payment_mode = '6' THEN 'Mobikwik'
		 WHEN A.Payment_mode = '7' THEN 'CC-Avenue-International'
		 END AS Payment_Mode
from
(
select details.order_id as Order_Id,
	details.vendor,
	details.owner_id,
	details.Order_Date,
	details.Payment_Date,
	details.Sales_Id,
	details.item_id as Item_Id,
	details.Item_Category as Item_Category,
	details.Item_Level as Item_Level,
	details.Item_Name as Item_Name,
	if(details.CandidateName is null or details.CandidateName='', 'Not Available',details.CandidateName) as Candidate_Name,
	if(details.CandidateEmail is null or details.CandidateEmail='', 'Not Available',details.CandidateEmail) as Candidate_Email,
	if(details.CandidateLocation is null or details.CandidateLocation='', 'Not Available',details.CandidateLocation) as Candidate_Location,
	if(details.CandidateContactNumber is null or details.CandidateContactNumber='', 'Not Available',details.CandidateContactNumber) as Candidate_ContactNumber,
	details.Payment_Mode,
	discinfo.GrossPrice_Order as GrossPrice_Order,
	discinfo.NetPrice_Order as NetPrice_Order,	
	discinfo.NetDiscount as EffectiveDiscount_Order,
	details.Item_Price as NetPrice_Item,
	(((100-discinfo.NetDiscount)/100)*details.Item_Price) as EffectivePrice_Item,
	#discinfo.DistributedDiscount as EffectiveDiscount_Item,
	discinfo.CouponCode as Coupon_Code,
	discinfo.CouponDiscount as Coupon_Discount
from 
		(
			select order_id,
				date(co.added_on) as Order_Date,
				date(co.payment_date) as Payment_Date,
				co.sales_id as Sales_Id,
				coi.price as Item_Price,
				coi.id as item_id,
				cc.name as Item_Category,
				coi.name as Item_Name,
				cpv.name as Item_Level,
				au.first_name as CandidateName,
				au.username as CandidateEmail,
				cup.city as CandidateLocation,
				cup.mobile as CandidateContactNumber,
				co.vendor,
				cp.owner_id,
				co.payment_mode as Payment_mode
			from shinecp.cart_orderitem as coi
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
			left join shinecp.cart_userprofile as cup
			on au.id=cup.user_id
			where date(co.added_on)>=date_add(curdate(), interval -31 day)
			and date(co.added_on)<date_add(curdate(), interval -0 day)
			and co.status=2
			order by 1 desc
		) as details
left join 
		(
			select oi.order_id,
				oi.Items_Count as items,
				ed.GrossPrice_Order,
				ed.NetPrice_Order,
				ed.NetDiscount,				
				IFNULL(ed.coupon_id,'No Discount Coupon Offered') as CouponCode,
				IFNULL(ed.Coupon_Discount,'0') as CouponDiscount,
				if(ed.NetDiscount=0,'0',round((1-pow(((100-ed.NetDiscount)/100),1/(oi.Items_Count)))*100,0)) as DistributedDiscount
			from 
			(
				#Calculating the number of items in each order
				select coi.order_id,
						count(*) as Items_Count
				from shinecp.cart_orderitem as coi
				left join shinecp.cart_order as co
				on coi.order_id=co.id
				#where date(co.added_on)>=date_add(curdate(), interval -31 day)
				#and date(co.added_on)<date_add(curdate(), interval -0 day)
				where co.status=2
				#where date(co.payment_date)>=date_add(curdate(), interval -31 day)
	            #            and co.status not in (1,6)
				group by 1
				order by 1 desc
			) as oi
			left join 
			(
				#Getting effective discount for each order
				select co.id as order_id,
					op.GrossPrice_Order,
					co.amount_payable as NetPrice_Order,
					if(op.GrossPrice_Order-co.amount_payable=0,'No Discount Offered',round(((op.GrossPrice_Order-co.amount_payable)/op.GrossPrice_Order)*100,0)) as NetDiscount,
					co.coupon_id,
					cc.discount as Coupon_Discount
				from shinecp.cart_order as co
				left join (select order_id,sum(price) as GrossPrice_Order from shinecp.cart_orderitem group by 1) as op
				on co.id=op.order_id
				left join shinecp.coupon_coupon as cc
				on co.coupon_id=cc.code
				order by 1 desc
			) as ed
			on oi.order_id=ed.order_id
			order by 1 desc
		) as discinfo
on details.order_id=discinfo.order_id
) as A
left join shinecp.auth_user as B
on A.owner_id=B.id