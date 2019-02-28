from marshmallow import Schema, fields, pprint,  ValidationError, validates
import validators


class posts_validate(Schema):
    domain          = fields.String(required=True)
    crawl_type      = fields.String()
    category        = fields.String()
    sub_category    = fields.Method('sub_category_val')
    thread_title    = fields.String()
    thread_url      = fields.Method('thread_url_val')
    post_id         = fields.String(required=True)
    post_url        = fields.Url()
    post_title      = fields.String()
    publish_epoch   = fields.Number()
    author          = fields.String()
    author_url      = fields.Method('author_url_val')
    post_text       = fields.String()
    all_links       = fields.Method('all_links_val')
    reference_url   = fields.Url()

    def sub_category_val(self,obj):
        sub_category_ = obj.get('sub_category')
        try:
            if sub_category_ !='':
                sub_category__ = eval(sub_category_)
                return sub_category_
            else:return sub_category_
        except:raise ValidationError('sub_category must be string witch contain list')

    def author_url_val(self,obj):
        author_url_ = obj.get('author_url')
        if author_url_ == '':
            return author_url_
        elif bool(validators.url(author_url_)):
            return author_url_
        else:raise ValidationError('author_url is not valid url')

    def thread_url_val(self,obj):
        thread_url_ = obj.get('thread_url')
        if thread_url_ != '':
            return thread_url_
        elif bool(validators.url(thread_url_)):
            return thread_url_
        else:raise ValidationError('thread_url is not valid url')

    def all_links_val(self,obj):
        links= obj.get('all_links')
        if links == '':
            raise ValidationError('all_links must be string witch contain list')
        else:
            try:
                links_ = eval(links)
                if links_ ==[]:return '[]'
                else:
                    for link in links_:
                        if bool(validators.url(link)):pass
                        else:raise ValidationError('all_links contain {} is not a url '.format(link))
                    return links
            except:raise ValidationError('all_links must be string witch contain list')

class author_validate(Schema):
    user_name        = fields.String(required=True)
    domain           = fields.String(required=True)
    crawl_type       = fields.String()
    author_signature = fields.String()
    join_date        = fields.Method('join_date_val')
    last_active      = fields.Method('last_active_val')
    total_posts      = fields.String()
    groups           = fields.String()
    reputation       = fields.String()
    credits          = fields.String()
    awards           = fields.String()
    rank             = fields.String()
    active_time      = fields.Method('active_time_val')
    contact_info     = fields.Method('contact_info_val')
    reference_url    = fields.Url()

    def join_date_val(self,obj):
        join_date = obj.get('join_date')
        if isinstance(join_date,int) or isinstance(join_date,float):
            return join_date
        elif join_date == '':return 0
        else:raise ValidationError('join_date most be int or "" ')

    def last_active_val(self,obj):
        last_active = obj.get('last_active')
        if isinstance(last_active,int) or isinstance(last_active,float):
            return last_active
        elif last_active == '':return 0
        else:raise ValidationError('last_active most be int or "" ')

    def active_time_val(self,obj):
        data = obj.get('active_time')
        test = self.list_of_dict(data)
        if test:
            return data
        else:raise ValidationError('active_time most be string having list of dict')

    def contact_info_val(self,obj):
        data = obj.get('contact_info')
        test = self.list_of_dict(data)
	if test:
            return data
        else:raise ValidationError('contact_info most be string having list of dict')

    def list_of_dict(self,data):
        try:
            data = eval(data)
            if data == []:return True
            else:
                for da in data:
                    if isinstance(da,dict) == False:
                        return False
                    else:return True
        except:return False



'''
#schema = posts_validate()
schema = author_validate()
result, error = schema.dump(val_a)
pprint(val_a)
pprint(result)
#import pdb;pdb.set_trace()
'''

