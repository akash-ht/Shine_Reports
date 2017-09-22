import os, os.path
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
import smtplib

sender = {-1:['analytics-no-reply','',''], 0:['analytics', '', '']}
server="172.22.65.51"
def send_mail(send_to, send_cc, send_bcc, subject, text, files=[], senderId=0):
#def send_mail(send_to, send_cc, send_bcc, subject, text, files=[], server="172.22.65.51", senderId=0):
    send_from = sender[senderId][0]+'@hindustantimes.com'
    #Reading authentication from file
    '''authenticationFile = "G:\shailk\Codes\SendMailUtil\\login.dat"
    fp = open(authenticationFile, "r")
    login = fp.read()
    fp.close()
    login1 = login.split("\n")'''

    assert type(send_to)==list
    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = COMMASPACE.join(send_cc)
    #msg['Bcc'] = COMMASPACE.join(send_bcc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(server, 587)
    #login = ['analytics','An@lyt1cS']
    login = [sender[senderId][1],sender[senderId][2]]
    #print login
    #smtp.login(login[0], login[1])
    smtp.sendmail(send_from, send_to + send_cc + send_bcc, msg.as_string())
    #print "mail Sent"
    smtp.close()
