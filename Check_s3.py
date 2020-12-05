import boto3
import json
import datetime
import pandas as pd
import sys
from io import StringIO
from collections import defaultdict
import pymysql as sql
import pymysql.cursors
from time import sleep
from urllib.request import Request, urlopen
import os
now = datetime.datetime.now()
#now = datetime.datetime(2020, 6, 2, 17, 0)
TABLE2 = 'CBONDFUTURESEODPRICES'
TABLE1 = 'CINDEXFUTURESEODPRICES'
TABLE = 'CCOMMODITYFUTURESEODPRICES'
tmpFile = '/home/ec2-user/tmp.txt'
writeinPath = 'wind_ready/'+now.strftime('%Y%m%d')+'.csv'
readPath = 'rollrule/' + now.strftime('%Y%m%d') + '.csv'
bucket = "wjx-future-dailyjob"
dd_hour = 17
dd_minute = 30
dd_second = 0

def check_tradedt(any_day):
    year = 2014
    holiday_2014 = [datetime.date(2014, 1, 1), datetime.date(2014, 1, 31), datetime.date(2014, 2, 3),
                    datetime.date(2014, 2, 4), datetime.date(2014, 2, 5), datetime.date(2014, 2, 6),
                    datetime.date(2014, 4, 7), datetime.date(2014, 5, 1), datetime.date(2014, 5, 2),
                    datetime.date(2014, 6, 1), datetime.date(2014, 9, 8), datetime.date(2014, 10, 2),
                    datetime.date(2014, 10, 3), datetime.date(2014, 10, 6), datetime.date(2014, 10, 7)]
    year = 2015
    holiday_2015 = [datetime.date(year, 1, 1), datetime.date(year, 1, 2), datetime.date(year, 2, 18),
                    datetime.date(year, 2, 19), datetime.date(year, 2, 20), datetime.date(year, 2, 23),
                    datetime.date(year, 2, 24), datetime.date(year, 4, 6), datetime.date(year, 5, 1),
                    datetime.date(year, 5, 1), datetime.date(year, 6, 22), datetime.date(year, 9, 3),
                    datetime.date(year, 9, 4), datetime.date(year, 10, 1), datetime.date(year, 10, 2),
                    datetime.date(year, 10, 5), datetime.date(year, 10, 6), datetime.date(year, 10, 7)]
    year = 2016
    holiday_2016 = [datetime.date(year, 1, 1), datetime.date(year, 2, 8), datetime.date(year, 2, 9),
                    datetime.date(year, 2, 10), datetime.date(year, 2, 11), datetime.date(year, 2, 12),
                    datetime.date(year, 4, 4), datetime.date(year, 5, 2), datetime.date(year, 6, 9),
                    datetime.date(year, 6, 10), datetime.date(year, 9, 15), datetime.date(year, 9, 16),
                    datetime.date(year, 10, 3), datetime.date(year, 10, 4), datetime.date(year, 10, 5),
                    datetime.date(year, 10, 6), datetime.date(year, 10, 7)]
    year = 2017
    holiday_2017 = [datetime.date(year, 1, 2), datetime.date(year, 1, 27), datetime.date(year, 1, 30),
                    datetime.date(year, 1, 31), datetime.date(year, 2, 1), datetime.date(year, 2, 2),
                    datetime.date(year, 4, 2), datetime.date(year, 4, 3), datetime.date(year, 4, 4),
                    datetime.date(year, 5, 1), datetime.date(year, 5, 29), datetime.date(year, 5, 30),
                    datetime.date(year, 10, 2), datetime.date(year, 10, 3), datetime.date(year, 10, 4),
                    datetime.date(year, 10, 5), datetime.date(year, 10, 6)]
    year = 2018
    holiday_2018 = [datetime.date(year, 1, 1), datetime.date(year, 2, 15), datetime.date(year, 2, 16),
                    datetime.date(year, 2, 19), datetime.date(year, 2, 20), datetime.date(year, 2, 21),
                    datetime.date(year, 4, 5), datetime.date(year, 4, 6), datetime.date(year, 4, 30),
                    datetime.date(year, 5, 1), datetime.date(year, 6, 18), datetime.date(year, 9, 24),
                    datetime.date(year, 10, 1), datetime.date(year, 10, 2), datetime.date(year, 10, 3),
                    datetime.date(year, 10, 4), datetime.date(year, 10, 5), datetime.date(year, 12, 31)]
    year = 2019
    holiday_2019 = [datetime.date(year, 1, 1), datetime.date(year, 2, 4), datetime.date(year, 2, 5),
                    datetime.date(year, 2, 6), datetime.date(year, 2, 7), datetime.date(year, 2, 8),
                    datetime.date(year, 4, 5), datetime.date(year, 5, 1), datetime.date(year, 5, 2),
                    datetime.date(year, 5, 3), datetime.date(year, 6, 7), datetime.date(year, 9, 13),
                    datetime.date(year, 10, 1), datetime.date(year, 10, 2), datetime.date(year, 10, 3),
                    datetime.date(year, 10, 4), datetime.date(year, 10, 7)]
    year = 2020
    holiday_2020 = [datetime.date(year, 1, 1), datetime.date(year, 1, 24), datetime.date(year, 1, 27),
                    datetime.date(year, 1, 28), datetime.date(year, 1, 29), datetime.date(year, 1, 30),
                    datetime.date(year, 1, 31), datetime.date(year, 4, 6), datetime.date(year, 5, 1),
                    datetime.date(year, 5, 4), datetime.date(year, 5, 5), datetime.date(year, 6, 25),
                    datetime.date(year, 6, 26), datetime.date(year, 10, 1), datetime.date(year, 10, 2),
                    datetime.date(year, 10, 5), datetime.date(year, 10, 6), datetime.date(year, 10, 7)]
    holiday = holiday_2014 + holiday_2015 + holiday_2016 + holiday_2017 + holiday_2018 + holiday_2019 + holiday_2020
    if any_day.weekday() != 6 and any_day.weekday() != 5 and any_day not in holiday:
        return True
    else:
        return False


def is_index(symbol):
    index_syms = ['IF','IH', 'IC']
    return symbol.upper() in index_syms
def is_bond(symbol):
    bond_syms = ['T','TF']
    return symbol.upper() in bond_syms

def checkInDB(table, ticker, cur):
    checked = False
    sql_str = 'SELECT COUNT(*) FROM ' + table + ' WHERE TRADE_DT = "' + now.strftime(
        "%Y%m%d") + '" and S_DQ_CLOSE is not null and S_INFO_WINDCODE like "'+ticker+'%";'
    cur.execute(sql_str)
    for row in cur:
        tickers = row[0]
    if tickers == 1:
        checked = True
    else:
        pass
    return checked


def check_s3(now):
    s3 = boto3.client('s3')
    object = readPath
    csv_obj = s3.get_object(Bucket=bucket, Key=object)
    body = csv_obj['Body']
    csv_str = body.read().decode('utf-8')
    rullrow = csv_str.split()

    finished = False
    deadline = now.replace(hour=dd_hour,minute=dd_minute,second=dd_second)
    talbe_list ={'index': TABLE1, 'bond': TABLE2, 'commodity': TABLE}
    while (not finished) and (now is not deadline):
        dbUser = "nancyli"
        port = 3306
        dbPassword = "ssAsPp_92233"
        dbHost = 'wjx-prd-db-wind-nx-cluster.cluster-ro-cgapmdlyvedz.rds.cn-northwest-1.amazonaws.com.cn'
        conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPassword)

        with conn.cursor() as cur:
            cur.execute("USE wind;")
            table_check = defaultdict(list)
            finish_status = list()
            for ticker in rullrow:
                sym = ''.join(i for i in ticker if not i.isdigit())
                if is_index(sym):
                    table_check['index'].append(checkInDB(TABLE1, ticker.upper(), cur))
                elif is_bond(sym):
                    table_check['bond'].append(checkInDB(TABLE2,ticker.upper(), cur))
                else:
                    table_check['commodity'].append(checkInDB(TABLE,ticker.upper(), cur))
            if os.path.exists(tmpFile):
                os.remove(tmpFile)
            file_write_obj = open(tmpFile,'w')
            for table_s in table_check:
                finish_status.append(all(table_check[table_s]))
                if all(table_check[table_s]):
                    file_write_obj.writelines(talbe_list[table_s])
                    file_write_obj.write('\n')
            file_write_obj.close()
                
            #s3.put_object(Body=str(obj_t), Bucket=bucket, Key=writeinPath)
            with open(tmpFile, 'rb') as f:
                s3.upload_fileobj(f, bucket, writeinPath)
            #s3.Bucket(bucket).upload_file('/home/ec2-user/tmp.txt',writeinPath)
            finished = all(finish_status)
            if not finished: sleep(60)
        conn.close()
    if finished:
        if os.path.exists(tmpFile):
            os.remove(tmpFile)
        payload = {'text': "finish uploading to s3"}
    else:
        payload = {'text': "not finishing uploading to s3"}
    url = "https://hooks.slack.com/services/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"
    request = Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
    response = urlopen(request)

if __name__ == '__main__':

    #now = datetime.datetime.now()
    if check_tradedt(now):
        check_s3(now)
    else:
        pass








