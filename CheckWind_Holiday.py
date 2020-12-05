import os
import pymysql as sql
import logging
#import boto3
import pymysql.cursors
from os import environ
#import barra_env
import re
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import time
import calendar
import json
import ssl
#import pandas as pd
import datetime


table_0 = "ASHAREEODPRICES"
table = 'AINDEXMEMBERS'
stockinfo = ['SSE50', 'CSI100', 'CSI300', 'CSI500', 'CSI800', 'CSI1000']
criteria = [221, 293, 993, 1812, 2211, 2098]
WebhookUrl = 'https://hooks.slack.com/services/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' #data-systems
#WebhookUrl = 'https://hooks.slack.com/services/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' #testing-for-nancy

with open('calendar.json', 'r') as f:
    calendar = json.load(f)
    

def get_index_ticker(index):
    res = dict()
    res['SSE50']   = '000016.SH'  ## [1,   50]   in SSE
    res['CSI100']  = '000903.SH'  ## [1,   100]  in SSE & SZSE
    res['CSI200']  = '000904.SH'  ## [101, 300]  in SSE & SZSE
    res['CSI300']  = '000300.SH'  ## [1,   300]  in SSE & SZSE
    res['CSI500']  = '000905.SH'  ## [301, 800]  in SSE & SZSE
    res['CSI800']  = '000906.SH'  ## [1,   800]  in SSE & SZSE
    res['CSI1000'] = '000852.SH'  ## [801, 1800] in SSE & SZSE
    assert index in res.keys(), 'unsupported index ' + index
    return res[index]

def check_tradedt(any_day):
    if any_day.strftime("%Y%m%d") in calendar:
        return True
    else:
        return False


def last_day_of_month(any_day):
    year = 2014
    holiday_2014 = [datetime.date(2014, 1, 1), datetime.date(2014, 1, 31), datetime.date(2014, 2, 3),
               datetime.date(2014, 2, 4), datetime.date(2014, 2, 5), datetime.date(2014, 2, 6),
               datetime.date(2014, 4, 7), datetime.date(2014, 5, 1), datetime.date(2014, 5, 2),
               datetime.date(2014, 6, 1), datetime.date(2014, 9, 8), datetime.date(2014, 10, 2),
                datetime.date(2014, 10, 3),datetime.date(2014, 10, 6), datetime.date(2014, 10, 7)]
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
    holiday_2019 = [ datetime.date(year, 1, 1), datetime.date(year, 2, 4), datetime.date(year, 2, 5),
                datetime.date(year, 2, 6), datetime.date(year, 2, 7), datetime.date(year, 2, 8),
                datetime.date(year, 4, 5), datetime.date(year, 5, 1), datetime.date(year, 5, 2),
                datetime.date(year, 5, 3), datetime.date(year, 6, 7), datetime.date(year, 9, 13),
            datetime.date(year, 10, 1),datetime.date(year, 10, 2), datetime.date(year, 10, 3),
                datetime.date(year, 10, 4),datetime.date(year, 10, 7)]
    year = 2020
    holiday_2020 = [datetime.date(year, 1, 1), datetime.date(year, 1, 24), datetime.date(year, 1, 27),
               datetime.date(year, 1, 28), datetime.date(year, 1, 29), datetime.date(year, 1, 30),
               datetime.date(year, 1, 31), datetime.date(year, 4, 6), datetime.date(year, 5, 1),
               datetime.date(year, 5, 4), datetime.date(year, 5, 5), datetime.date(year, 6, 25),
               datetime.date(year, 6, 26), datetime.date(year, 10, 1), datetime.date(year, 10, 2),
               datetime.date(year, 10, 5), datetime.date(year, 10, 6), datetime.date(year, 10, 7)]
    holiday = holiday_2014 + holiday_2015 + holiday_2016 + holiday_2017 + holiday_2018 + holiday_2019 + holiday_2020

    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    lastdate = next_month - datetime.timedelta(days=next_month.day)
    lastday = lastdate.day
    if lastdate.weekday() == 6:
        lastday -= 2
    elif lastdate.weekday() == 5:
        lastday -= 1
    while datetime.date(any_day.year, any_day.month, lastday)  in holiday:
        lastday -= 1
    return lastday

class SendToSNS:
    def __init__(self, msg, tables):
        self.msg = msg
        self.tables = tables
        self.timestamp_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        # if self.msg['StateValue'] == "ALARM":
        #     self.color = "danger"
        # elif self.msg['StateValue'] == "OK":
        #     self.color = "good"

    def slack_data(self):
        if check_tradedt(datetime.datetime.strptime(self.msg['date'],"%Y-%m-%d")):
            fields = []
          #  fields = [{"value": self.msg[table_0], "short": False}]
            for table in self.msg:
                fields.append({"value": self.msg[table], "short": False})

            _message = {
                'attachments': [
                    {
                        'title': '---Wind Index Check---\n' +
                                 '[' + table_0 +' and AINDEXMEMBERS ]\n' +
                                 '[' + datetime.datetime.now().strftime(
                            "%Y-%m-%d-%I:%M:%S %p") + ']',  #
                        'fields': fields
                    }
                ]
            }

        else: # not trading day
            _message = {
                'attachments': [
                    {
                        'title': ' --- Index check ---',
                        'fields': [
                            {
                                "value": ':white_check_mark:[ today ' + self.msg[
                                    'date'] + '] is not trading day',
                                "short": False
                            }
                        ]
                    }
                ]
            }
        return _message


def selectFromDBiHoliday(date_today):
    #  connect to sql server
    dbUser = "nancyli"
    port = 3306
    dbPassword = "ssAsPp_92233"
    dbHost = "wjx-production-db-wind-bj-01.ctrp3nrqkmbu.rds.cn-north-1.amazonaws.com.cn"
    dbname = "nancyli"
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPassword)
    msg_t = []
    tickers = []
    with conn.cursor() as cur:
        cur.execute("USE wind")
        sql_str = "SELECT  COUNT(*)  FROM "+ table_0 +" where TRADE_DT='"+date_today.strftime("%Y%m%d")+"';"
        cur.execute(sql_str)
        for row in cur:
            tickers_0 = row[0]
        if int(tickers_0) > 3750:
            msg_t.append(1)
        else:
            msg_t.append(0)


        for ii, stock in enumerate(stockinfo):
            stock_t = get_index_ticker(stock)
            # check sse50 for AINDEXMEMBERS at 10::00
            sql_str = "SELECT  COUNT(*)  FROM "+table+" where S_INFO_WINDCODE='"+stock_t+"';"
            cur.execute(sql_str)
            for row in cur:
                tickers_t = row[0]
                tickers.append(tickers_t)
            if tickers_t>=criteria[ii]:
                #print(1)
                msg_t.append(1)
            else:
                print("SSE50:"+str(tickers_t) +" less than 221 ERROR !!")
                msg_t.append(0)
    conn.close()
    if all(msg_t):
        msg = {date_today.strftime("%Y%m%d"):':white_check_mark: '+ date_today.strftime("%Y%m%d")+ " OK"}
    else:
        msg = {date_today.strftime("%Y%m%d"):':warning: ' + date_today.strftime("%Y%m%d")+  "NOT OK"}

    return msg


if __name__ == '__main__':
    #today = datetime.datetime.now()
    nextTradedate = datetime.datetime.now() + datetime.timedelta(days=1)
    #today = datetime.date(2020,6,28)
    #url = "https://hooks.slack.com/services/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"
    url = WebhookUrl
    msg_t = {}
    if nextTradedate.strftime("%Y%m%d") in calendar:
        date_index = calendar.index(nextTradedate.strftime("%Y%m%d"))
        for trade_date in calendar[date_index - 14:date_index-1]:
            check_date = datetime.datetime.strptime(trade_date,"%Y%m%d")
            msg= selectFromDBiHoliday(check_date)
            msg_t.update(msg)
        msg_t.update({'date': nextTradedate.strftime("%Y-%m-%d")})
        slack_msg = json.dumps(msg_t)
        slack_data = SendToSNS(json.loads(slack_msg),calendar[date_index - 14:date_index-1]).slack_data()
        request = Request(
            url,
            data=json.dumps(slack_data).encode(),
            headers={'Content-Type': 'application/json'}
        )
        ssl._create_default_https_context = ssl._create_unverified_context
        response = urlopen(request)
