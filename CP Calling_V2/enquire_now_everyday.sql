select email, url
from marketing_campaign where email is not null and email != ' ' and email != '' and created_on >= DATE_SUB(CURDATE(),INTERVAL 1 DAY) and created_on < CURDATE()
