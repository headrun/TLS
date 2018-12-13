

import re
import unicodedata

import configuration
import datetime


# ToDo: get this column names dynamically
def generate_upsert_query_authors(crawler):
    table_name = configuration.tables[crawler]['authors']
    upsert_query = """ INSERT INTO {0} (user_name, domain, crawl_type, author_signature, join_date, last_active, \
                    total_posts, fetch_time, groups, reputation, credits, awards, rank, active_time, contact_info,\
                    reference_url) VALUES ( %(user_name)s, %(domain)s, %(crawl_type)s, %(author_signature)s, \
                    %(join_date)s, %(last_active)s, %(total_posts)s, %(fetch_time)s, %(groups)s, %(reputation)s,\
                    %(credits)s, %(awards)s, %(rank)s, %(active_time)s, %(contact_info)s, %(reference_url)s )\
                    ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s, author_signature=%(author_signature)s, \
                    join_date=%(join_date)s, last_active=%(last_active)s, total_posts=%(total_posts)s,\
                    fetch_time=%(fetch_time)s, groups=%(groups)s, reputation=%(reputation)s, credits=%(credits)s,\
                    awards=%(awards)s, rank=%(rank)s, active_time=%(active_time)s, contact_info=%(contact_info)s,\
                    reference_url=%(reference_url)s """.format(table_name)

    return upsert_query


def generate_upsert_query_posts(crawler):
    table_name = configuration.tables[crawler]['posts']
    upsert_query = """INSERT INTO {0} (domain, crawl_type, category, sub_category, thread_title, thread_url, post_id,\
                    post_url, post_title, publish_epoch, fetch_epoch, author, author_url, post_text, all_links, reference_url)\
                    VALUES( %(domain)s, %(crawl_type)s, %(category)s, %(sub_category)s, %(thread_title)s, %(thread_url)s,\
                    %(post_id)s, %(post_url)s, %(post_title)s, %(publish_epoch)s, %(fetch_epoch)s, %(author)s, %(author_url)s,\
                    %(post_text)s, %(all_links)s, %(reference_url)s) ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s,\
                    category=%(category)s, sub_category=%(sub_category)s, thread_title=%(thread_title)s, \
                    thread_url=%(thread_url)s, post_url=%(post_url)s, post_title=%(post_title)s, publish_epoch=%(publish_epoch)s,\
                    fetch_epoch=%(fetch_epoch)s, author=%(author)s, author_url=%(author_url)s, post_text=%(post_text)s,\
                    all_links=%(all_links)s, reference_url=%(reference_url)s """.format(table_name)

    return upsert_query

def generate_upsert_query_crawl(crawler):
    #import pdb;pdb.set_trace()
    table_name = configuration.tables[crawler]['crawl']

    upsert_query = """INSERT INTO {0} (post_id, auth_meta, links) VALUES(%(post_id)s, %(auth_meta)s, %(links)s) \
                    ON DUPLICATE KEY UPDATE auth_meta=%(auth_meta)s, links=%(links)s """.format(table_name)
    return upsert_query


def prepare_links(links_in_post):
    """
    converts links_in_post in the format required by the client.
    :param links_in_post:
    :return:
    """
    aggregated_links = []
    for link in links_in_post:
        aggregated_links.append(link)
    if not aggregated_links:
        all_links = '[]'
    else:
        all_links = list(set(aggregated_links))
        all_links = str(all_links)
    return all_links


def clean_spchar_in_text(text):
    """
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    """
    # import pdb; pdb.set_trace()
    # get text in unicode format
    """
    try:
        text = unicode(text, 'utf-8')
    except TypeError:
        pass
    text = unicodedata.normalize("NFKD", text)
    """
    # text = unicodedata.normalize("NKFD", text).encode('ascii', 'ignore')
    # text = unicodedata.normalize('NFKD', text.decode('utf8')).encode('utf8')
    # ToDo: Fix the order of transformations.
    # Note: Work In Progress
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    text = re.sub(r'(\n\s*)', '\n', text)
    # text = re.sub(r'(\n\s*)', '\n', text).encode('utf-8').encode('ascii', 'ignore')
    # text = text.encode('ascii', 'ignore')
    text = text.replace('<br>', '\n')
    text = re.sub('\s\s+', ' ', text)
    return text


def clean_url(unclean_url):
        '''
        Removes the trailing '/?' or '/' in a given url scheme.
        '''
        cleaned_url = re.sub(r'(\/\?|\/)$', '', unclean_url)
        return cleaned_url


def get_epoch(dt=datetime.datetime.utcnow()):
    '''

    :param datetimestamp:
    :return:
    '''
    epoch = int(calendar.timegm(dt.utctimetuple())) * 1000
    return epoch


def clean_text(input_text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text


def get_aggregated_links(links):
    if not links:
        aggregated_links = str([])
    else:
        aggregated_links = str(list(set(links)))

    return aggregated_links




if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
