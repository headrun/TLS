

TO MOVE ONE CRAWLER DATA FROM MYSQL TO ES:

python extract_dump.py --DB_SCHEMA hellbound --TABLE_NAME_AUTHORS hellbound_authors --TABLE_NAME_POSTS hellbound_posts --DB_CHARSET utf8 --RECS_BATCH 10


TO MOVE ALL CRAWLERS DATA FROM MYSQL TO ES
dump_config.py         #CONTAINS ALL CRAWLERS INFO
python dump_to_es.py   # USED TO MOVE DATA 


FETCH_RESULT
python fetch_results.py --INDEX_FORUM 2 --DOMAIN www.hellboundhackers.org
