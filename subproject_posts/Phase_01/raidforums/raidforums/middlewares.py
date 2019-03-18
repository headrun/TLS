from w3lib.http import basic_auth_header
import random
import user_agent
user_agent_list = user_agent.user_agents.split('\n')

class CustomProxyMiddleware(object):
    def process_request(self, request, spider):
        username = 'lum-customer-headrunp3-zone-tls'#'lum-customer-headrunp3-zone-tls_mobil'
        password = 'f3h6o6ru8beq'#'2m53ycary4yb'#'YOURPASS'
        port = 22225
	session_id = random.choice(range(1,31))
	super_proxy_url = 'https://%s-country-us-session-%s:%s@zproxy.lum-superproxy.io:%d' % (username, session_id, password, port)
        request.meta['proxy'] = super_proxy_url 
        ua = random.choice(user_agent_list)
        request.headers['User-Agent']=ua

