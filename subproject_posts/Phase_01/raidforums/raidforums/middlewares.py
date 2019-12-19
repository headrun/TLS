from w3lib.http import basic_auth_header
import random
import user_agent
user_agent_list = user_agent.user_agents.split('\n')
from w3lib.http import basic_auth_header
proxy_list = ['fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com','centralusa.torguardvpnaccess.com','west.usa.torguardvpnaccess.com','westusa.torguardvpnaccess.com','east.usa.torguardvpnaccess.com','eastusa.torguardvpnaccess.com']+ ['au.torguardvpnaccess.com', 'melb.au.torguardvpnaccess.com', 'bul.torguardvpnaccess.com', 'cp.torguardvpnaccess.com', 'egy.torguardvpnaccess.com', 'iom.torguardvpnaccess.com', 'isr.torguardvpnaccess.com', 'fin.torguardvpnaccess.com', 'br.torguardvpnaccess.com', 'ca.torguardvpnaccess.com', 'vanc.ca.west.torguardvpnaccess.com', 'frank.gr.torguardvpnaccess.com', 'ice.torguardvpnaccess.com', 'ire.torguardvpnaccess.com', 'in.torguardvpnaccess.com', 'jp.torguardvpnaccess.com', 'nl.torguardvpnaccess.com', 'lon.uk.torguardvpnaccess.com', 'ro.torguardvpnaccess.com', 'ru.torguardvpnaccess.com', 'mos.ru.torguardvpnaccess.com', 'swe.torguardvpnaccess.com', 'swiss.torguardvpnaccess.com', 'bg.torguardvpnaccess.com', 'hk.torguardvpnaccess.com', 'cr.torguardvpnaccess.com', 'hg.torguardvpnaccess.com', 'my.torguardvpnaccess.com', 'thai.torguardvpnaccess.com', 'turk.torguardvpnaccess.com', 'tun.torguardvpnaccess.com', 'mx.torguardvpnaccess.com', 'singp.torguardvpnaccess.com', 'saudi.torguardvpnaccess.com', 'fr.torguardvpnaccess.com', 'pl.torguardvpnaccess.com', 'czech.torguardvpnaccess.com', 'gre.torguardvpnaccess.com', 'it.torguardvpnaccess.com', 'sp.torguardvpnaccess.com', 'no.torguardvpnaccess.com', 'por.torguardvpnaccess.com', 'za.torguardvpnaccess.com', 'den.torguardvpnaccess.com', 'vn.torguardvpnaccess.com', 'sk.torguardvpnaccess.com', 'lv.torguardvpnaccess.com', 'lux.torguardvpnaccess.com', 'nz.torguardvpnaccess.com', 'md.torguardvpnaccess.com', 'uae.torguardvpnaccess.com', 'slk.torguardvpnaccess.com', 'fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com', 'centralusa.torguardvpnaccess.com', 'west.usa.torguardvpnaccess.com', 'westusa.torguardvpnaccess.com', 'east.usa.torguardvpnaccess.com', 'eastusa.torguardvpnaccess.com']


class CustomProxyMiddleware(object):


    def process_request(self, request, spider):
        username = 'lum-customer-headrunp3-zone-tls'#'lum-customer-headrunp3-zone-tls_mobil'
        password = 'f3h6o6ru8beq'#'2m53ycary4yb'#'YOURPASS'
        port = 22225
	#session_id = random.choice(range(1,31))
	proxy = random.choice(proxy_list)
	#super_proxy_url = 'https://%s-country-us-session-%s:%s@zproxy.lum-superproxy.io:%d' % (username, session_id, password, port)
        request.meta['proxy'] = 'http://'+ proxy+':6060'# 'http://fl.east.usa.torguardvpnaccess.com:6060'#super_proxy_url 
	request.headers['Proxy-Authorization'] = basic_auth_header('vinuthna@headrun.com','Hotthdrn591!')#'Basic dmludXRobmFAaGVhZHJ1bi5jb206SG90dGhkcm41OTEh'
        ua = random.choice(user_agent_list)
        request.headers['User-Agent']=ua
