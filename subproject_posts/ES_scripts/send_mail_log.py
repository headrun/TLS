import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

import optparse

parser = optparse.OptionParser()
parser.add_option("--BODY",dest = "BODY",help = "enter BODY")
parser.add_option("--SUBJECT",dest = "SUBJECT",help = "enter SUBJECT")
(options,arguments) = parser.parse_args()

SUBJECT = options.SUBJECT
if SUBJECT == None:
    SUBJECT = "Recaptcha is not working"
if SUBJECT == "Recaptcha is not working":
   TO = 'saikeerthi@headrun.com,sreenivas.dega@headrun.com'
else:
   TO = 'saikeerthi@headrun.com,sreenivas.dega@headrun.com,srikanths@threatlandscape.com'
BODY = options.BODY

def send_mail():
    strFrom = 'saikrishnatls2019@gmail.com'
    strTo = TO
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = SUBJECT
    msgRoot['From'] = strFrom
    msgRoot['To'] = strTo
    body = BODY
    body = MIMEText(body)
    msgRoot.attach(body)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('saikrishnatls2019@gmail.com', 'saikrishna123')
    server.sendmail('saikrishnatls2019@gmail.com', strTo, msgRoot.as_string())


if __name__ == "__main__":
    send_mail()


