select 
	IF(b.first_name is null or b.first_name='','Not Available',b.first_name) as Candidate_Name,
	b.email as Candidate_Email,
	c.mobile as Candidate_Mobile,
	IFNULL(a.transaction_id,'Not Available') as Transaction_Id,
	IFNULL(a.instrument_number,'Not Available') as Instrument_Number,
	IF(a.payment_mode=0,'Not Paid',IF(a.payment_mode=1,'Cash',IF(a.payment_mode=2,'Citrus Pay',IF(a.payment_mode=3,'EMI',IF(a.payment_mode=4,'Cheque/Draft','CC-Avenue'))))) as Pay_Mode,
	IF(a.status=1,'Un Paid',IF(a.status=2,'Paid','Lead')) as Payment_Status
from shinecp.cart_order as a
left join shinecp.auth_user as b
on a.candidate_id=b.id
left join shinecp.cart_userprofile as c
on c.user_id=b.id
where a.status in (1,2,6)
and date(a.added_on)=date_add(curdate(), interval -1 day)
and c.mobile is not null
and (c.mobile<>'' or b.email<>'')
group by 1,2,3,4,5,6,7
order by 1
