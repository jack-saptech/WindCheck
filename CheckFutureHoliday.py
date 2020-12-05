#!/usr/bin/python3
import os
import pymysql as sql
import logging
# import boto3
import pymysql.cursors
from os import environ
# import barra_env
import re
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import time
import calendar
import json
# import pandas as pd
import datetime
import ssl
from time import sleep
import pickle

import requests
from slack.errors import SlackApiError


#WebhookUrl='https://hooks.slack.com/services/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' #testing-for-nancy
#WebhookUrl = 'https://hooks.slack.com/services/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' #log=data-futures-prod
#WebhookUrl = 'https://hooks.slack.com/services/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' #log-data-equity-prod
WebhookUrl = 'https://hooks.slack.com/services/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' #data-systems
TABLES=['CFUTURESWAREHOUSESTOCKS','CFUTURESINSTOCK','CCOMMODITYFUTURESEODPRICES','CCOMMODITYFUTURESPOSITIONS',
        'CINDEXFUTURESEODPRICES','CINDEXFUTURESPOSITIONS','CBONDFUTURESEODPRICES','CBONDFUTURESPOSITIONS']

ref=['ANN_DATE','ANN_DATE','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT']

Threshold=[823, 50, 1800, [7500,52], 30, 420, 24, 180]
slack_channel='testing-for-nancy'
with open('calendar.json', 'r') as f:
    calendar = json.load(f)

def sendAPI(errormsg):
    api_endpoint = 'http://ec2-52-81-50-220.cn-north-1.compute.amazonaws.com.cn:5000'

    create_ticket_api = '/api/v1/alert.create'
    ticket_data = {"Slack_Channel": slack_channel, "Error_Summary": "wind future check error",
                   "Error_Time": datetime.datetime.timestamp(datetime.datetime.now()),
                   "Error_Message": errormsg}


    # using json.dumps is to send the Chinese correctly

    #send_data = json.dumps(ticket_data, ensure_ascii=False)
    send_data = pickle.dumps(ticket_data)

    resp = requests.post(api_endpoint + create_ticket_api, data=send_data,
                         headers={'Content-type': 'text/plain; charset=utf-8'})


def check_tradedt(any_day):
        tradedate = calendar
        if any_day.strftime('%Y%m%d') in tradedate:
            return True
        else:
            return False

class SendToSNS:
    def __init__(self, msg,tables):
        self.msg = msg
        self.tables = tables
        self.timestamp_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        # if self.msg['StateValue'] == "ALARM":
        #     self.color = "danger"
        # elif self.msg['StateValue'] == "OK":
        #     self.color = "good"

    def slack_data(self):

        if check_tradedt(datetime.datetime.strptime(self.msg['date'], "%Y-%m-%d")):
            fields = []
            for table in self.msg:
                fields.append({"value": self.msg[table], "short": False})
                #fields.append({"value": "data from" + self.msg['date'], "short": False})

            _message = {
                'attachments': [
                    {
                        'title': '---Wind FUTURE Check---\n' + '[' + datetime.datetime.now().strftime(
                            "%Y-%m-%d-%I:%M:%S %p") + ']',  #
                        'fields': fields
                    }
                ]
            }
        else:
            _message = {
                'attachments':[
                    {
                        'title':' --- WIND FUTURE check ---',
                        'fields':[
                            {
                                "value":':white_check_mark:[ today ' + self.msg['date'] + '] is not trading day',
                                "short":False
                            }
                        ]
                    }
                ]
            }
        return _message


def selectFromDB_holiday(today_dt):
    #  connect to sql server
    dbUser = "nancyli"
    port = 3306
    dbPassword = "ssAsPp_92233"
    dbHost = "wjx-production-db-wind-bj-01.ctrp3nrqkmbu.rds.cn-north-1.amazonaws.com.cn"
    dbname = "nancyli"
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPassword)

    tickers = []
    msg_t = []
    error_msg = []
    error_table = []
    error_thre = []
    with conn.cursor() as cur:
        cur.execute("USE wind")
        for index,table in enumerate(TABLES):
            sql_str = "SELECT  COUNT(*)  FROM " + table + " where "+ref[index]+ "='" + today_dt.strftime("%Y%m%d") + "';"
            cur.execute(sql_str)
            for row in cur:
                tickers_0 = row[0]
            if index == 3:
                sql_str = "SELECT  COUNT(DISTINCT S_INFO_WINDCODE_PREFIX)  FROM " + table + " where "+ref[index]+ "='" + today_dt.strftime("%Y%m%d") + "';"
                cur.execute(sql_str)
                for row in cur:
                    tickers_1 = row[0]
                if int(tickers_0) >= Threshold[index][0] and int(tickers_1) >= Threshold[index][1]:
                    msg_t.append(1)
                elif int(tickers_0) < Threshold[index][0] and int(tickers_1) >= Threshold[index][1]:
                    msg_t.append(0)
                elif int(tickers_0) >= Threshold[index][0] and int(tickers_1) < Threshold[index][1]:
                    msg_t.append(1)
                elif int(tickers_0) < Threshold[index][0] and int(tickers_1) < Threshold[index][1]:
                    msg_t.append(0)
                    error_table.append(table)
                    error_thre.append(Threshold[index])
            else:
                if int(tickers_0) >= Threshold[index]:
                    msg_t.append(1)
                else:
                    msg_t.append(0)
                    error_msg.append( table + ": " + str(tickers_0) + " less than  "+ str(Threshold[index]) + " ERROR!")
                    error_table.append(table)
                    error_thre.append(Threshold[index])
    conn.close()
    msg = {}
    url = WebhookUrl
    if all(msg_t):
        msg = {today_dt.strftime("%Y%m%d"):':white_check_mark: '+ today_dt.strftime("%Y%m%d")+ " OK"}
    else:
        msg = {today_dt.strftime("%Y%m%d"):':warning: ' + today_dt.strftime("%Y%m%d")+  "NOT OK"}

    return msg

def reviewError(errormsg, errortable):
    msg_t = []
    dbUser = "nancyli"
    port = 3306
    dbPassword = "ssAsPp_92233"
    dbHost = "wjx-production-db-wind-bj-01.ctrp3nrqkmbu.rds.cn-north-1.amazonaws.com.cn"
    dbname = "nancyli"
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPassword)
    with conn.cursor() as cur:
        cur.execute("USE wind")
        while errortable != []:
            sleep(60 * 5)
            for ii, tt in enumerate(errortable):
                if tt == 'CFUTURESWAREHOUSESTOCKS' or tt == 'CFUTURESINSTOCK':
                    ref = 'ANN_DATE'
                else:
                    ref = 'TRADE_DT'
                sql_str = "SELECT  COUNT(*)  FROM " + tt + " where " + ref + "='" + today_dt.strftime(
                    "%Y%m%d") + "';"
                cur.execute(sql_str)
                for row in cur:
                    tickers_0 = row[0]
                if tt == "CCOMMODITYFUTURESPOSITIONS":
                    sql_str = "SELECT  COUNT(DISTINCT S_INFO_WINDCODE_PREFIX)  FROM " + tt + " where " + ref + "='" + today_dt.strftime("%Y%m%d") + "';"
                    cur.execute(sql_str)
                    for row in cur:
                        tickers_1 = row[0]
                    if int(tickers_0) >= errorthre[0] and int(tickers_1) >= errorthre[1]:
                        msg_t.append(
                            ":white_check_mark:" + tt + ": " + str(tickers_0) + ">" + str(
                                errorthre[0]) + " \n (:white_check_mark: S_INFO_WINDCODE_PREFIX >" + str(
                                errorthre[1]) + ") OK!")
                    elif int(tickers_0) < errorthre[0] and int(tickers_1) >= errorthre[1]:
                        msg_t.append(
                            ":warning:" + tt + ": " + str(tickers_0) + " less than" + str(
                                errorthre[0]) + " \n( :white_check_mark: S_INFO_WINDCODE_PREFIX >" + str(
                                errorthre[1]) + ")")
                    elif int(tickers_0) >= errorthre[0] and int(tickers_1) < errorthre[1]:
                        msg_t.append(
                            ":white_check_mark:" + tt + ": " + str(tickers_0) + " > " + str(
                                errorthre[0]) + " \n ( :warning: S_INFO_WINDCODE_PREFIX less than" + str(
                                errorthre[1]) + ")")
                    elif int(tickers_0) < errorthre[0] and int(tickers_1) < errorthre[1]:
                        msg_t.append(
                            ":warning:" + tt + ": " + str(tickers_0) + " less than " + str(
                                errorthre[0]) + " \n ( :warning: S_INFO_WINDCODE_PREFIX less than" + str(
                                errorthre[1]) + ") ")
                else:

                    if int(tickers_0) >= errorthre[ii]:
                        msg_t.append(":white_check_mark:" + tt + ": " + str(tickers_0) + ">" + str(errorthre[ii]) + " OK!")
                        errortable.remove(tt)
                    else:
                        msg_t.append(":warning:" + tt + ": " + str(tickers_0) + " less than  " + str(
                            errorthre[ii]) + " ERROR!")
                msg = {}
                url = WebhookUrl
            for index, table in enumerate(errortable):
                msg.update({table: msg_t[index]})
                msg.update({'date': today_dt.strftime("%Y-%m-%d")})
            slack_msg = json.dumps(msg)
            slack_data = SendToSNS(json.loads(slack_msg), errortable).slack_data()
            request = Request(
                url,
                data=json.dumps(slack_data).encode(),
                headers={'Content-Type': 'application/json'}
            )
            ssl._create_default_https_context = ssl._create_unverified_context
            response = urlopen(request)
        conn.close()


if __name__ == '__main__':
    nextTradedate = datetime.datetime.now() + datetime.timedelta(days = 1)
    url = WebhookUrl
    msg_t = {}
    if nextTradedate.strftime("%Y%m%d") in calendar:
        date_index = calendar.index(nextTradedate.strftime("%Y%m%d"))
        for trade_date in calendar[date_index - 14:date_index-1]:
            check_date = datetime.datetime.strptime(trade_date,"%Y%m%d")
            msg= selectFromDB_holiday(check_date)
            # if errormsg != []:
            #     sendAPI(errormsg)
            # reviewError(errormsg, errortable)
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
    else:
        msg = {"date": nextTradedate.strftime("%Y-%m-%d")}

        url = WebhookUrl
        msg_t = json.dumps(msg)

        slack_data = SendToSNS(json.loads(msg_t),TABLES).slack_data()
        request = Request(
            url,
            data=json.dumps(slack_data).encode(),
            headers={'Content-Type': 'application/json'}
        )
        ssl._create_default_https_context = ssl._create_unverified_context
        response = urlopen(request)
        errormsg = []



