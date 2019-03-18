from w3lib.http import basic_auth_header

import random, user_agent
user_agent_list = user_agent.user_agents.split('\n')

class CustomProxyMiddleware(object):
    def process_request(self, request, spider):
	username = 'lum-customer-headrunp3-zone-tls'#'lum-customer-CUSTOMER-zone-YOURZONE_STATIC-route_err-block'
        password = 'f3h6o6ru8beq'#'YOURPASS'
        port = 22225
        session_id = random.choice(range(1,31))
        super_proxy_url = 'http://%s-country-us-session-%s:%s@zproxy.lum-superproxy.io:%d' % (username, session_id, password, port)
        request.meta['proxy'] = super_proxy_url
        ua = random.choice(user_agent_list)
        request.headers['User-Agent']=ua

	'''
        proxy_list = ['96.47.226.98','96.44.147.34','96.44.147.122','96.44.147.138','96.44.146.106']
        request.meta['proxy'] = 'https://{0}:6060'.format(random.choice(proxy_list))
        request.headers['Proxy-Authorization'] = basic_auth_header('hr@headrun.com','hdrn^123!')
	'''
