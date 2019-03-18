from elasticsearch import Elasticsearch
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import time
from datetime import datetime
from time import sleep
x = '''www.raidforums.com,1500
www.kernelmode.info,500
forum.exetools.com,200
www.board.b-at-s.info,0
www.bleepingcomputer.com,2000
forums.malwarebytes.com,300
www.antionline.com,100
www.hackforums.net,0
www.hellboundhackers.org,50
www.blackhatworld.com,2000
www.nulled.to,100
sky-fraud.ru,200
prologic.su,10
www.forum.bits.media,100
monopoly.ms,1
www.at4re.net,5
mvfjfugdwgc5uwho.onion,1
binrev.com,100
www.wilderssecurity.com,1000
www.hackthissite.org,100'''
from pprint import pprint 


class Count_in_es:

    def vals(self):   
        self.es = Elasticsearch(['10.2.0.90:9342'])
	bodys = []
	for domains in x.split('\n'):
	    domain,baseline = domains.split(',')
	    domain_data = []
	    now = round((time.time()- time.timezone)*1000)
	    for i in range(1,3):	    
		count = self.count_in_specific_date(domain,now)
		domain_data.append(count)
		now = now - 86400*i*1000
	    total = self.total_count(domain)
	    diff = []
	    [diff.append(domain_data[i]-domain_data[i+1]) for i in range(len(domain_data)-1)]
	    if diff[0]<int(baseline):
	    	#print domain,baseline,domain_data,diff
	    	body = self.send_mail(domain,baseline)#print domain,baseline,diff
		bodys.append(body)
	self.sending_mail(bodys)


    def count_in_two(self,now,domain):
	one_day_back = now-86400*1000
	que={"query":{"bool":{"must":[{"range":{"fetch_time":{"gte":one_day_back,"lt":now}}},{"match":{"domain":domain} }]}}}
	res = self.es.search(body=que)
	return res['hits']['total'] 

    def total_count(self,domain):
	que = { "query":{ "bool": {"must": [{ "match":{"domain" : domain}}]  }}}
        total_ = self.es.search(body=que)
        total = total_['hits']['total']
	return total

    def count_in_specific_date(self,domain,date_):
	#one_day_back = date_-86400*1000
	que={"query":{"bool":{"must":[{"range":{"fetch_time":{"lt":date_}}},{"match":{"domain":domain}}]}}}
	res = self.es.search(body=que)
	total = res['hits']['total']
	return total

    def send_mail(self, domain,baseline):
	data_flow = []
	now = round((time.time()- time.timezone)*1000)
	for i in range(1,8):
	    count = self.count_in_specific_date(domain,now)
    	    data_flow.append((time.strftime('%Y-%m-%d', time.localtime(now/1000)),count))
    	    now = now - 86400*1000
	    total = self.total_count(domain)
	body = body ='As per the daily update of the site,\nThe Baseline for ' + domain+ ' is less than the approxiamately baseline which we have taken.So,new data is not populated to elasticsearch for today \ndata flow as follows from past week \n'+str(data_flow)+'\ntotal count in ES='+str(total)
	return body

    def sending_mail(self,bodys):
	body = ('\n'+'_'*100+'\n').join(bodys)
    	strFrom = 'saikrishnatls2019@gmail.com'
	strTo = ['saikeerthi@headrun.com', 'sreenivas@headrun.com', 'srikanths@threatlandscape.com']
        msgRoot = MIMEMultipart('related')
	d = datetime.now()
	c_time = d.strftime('%m/%d/%Y %H:%M:%S')
    	msgRoot['Subject'] = 'Alerting mechanism per each forum in elasticsearch:'+ c_time
    	msgRoot['From'] = strFrom
    	msgRoot['To'] = ' ,'.join(strTo)
    	body = MIMEText(body)
    	msgRoot.attach(body)
    	server = smtplib.SMTP('smtp.gmail.com', 587)
    	server.starttls()
    	server.login('saikrishnatls2019@gmail.com', 'saikrishna123')
    	server.sendmail('saikrishnatls2019@gmail.com', strTo, msgRoot.as_string())


if __name__== "__main__":
    val = Count_in_es()
    val.vals()

