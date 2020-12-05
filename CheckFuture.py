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
import tushare as ts
import datetime
import ssl
from time import sleep
import pickle

import requests
from slack.errors import SlackApiError


#WebhookUrl='https://hooks.slack.com/services/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'
WebhookUrl = 'https://hooks.slack.com/services/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'

TABLES=['CFUTURESWAREHOUSESTOCKS','CFUTURESINSTOCK','CCOMMODITYFUTURESEODPRICES','CCOMMODITYFUTURESPOSITIONS',
        'CINDEXFUTURESEODPRICES','CINDEXFUTURESPOSITIONS','CBONDFUTURESEODPRICES','CBONDFUTURESPOSITIONS']

ref=['ANN_DATE','ANN_DATE','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT','TRADE_DT']

Threshold=[823, 50, 1800, [7500,52], 30, 420, 24, 180]
slack_channel='log-data-futures-prod'


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
    with open('/opt/monitor/calendar.json', 'r') as f:
        tradedate = json.load(f)
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
                fields.append({"value": "data from" + self.msg['date'], "short": False})

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


def selectFromDB(today_dt):
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
                    msg_t.append(
                        ":white_check_mark:" + table + ": " + str(tickers_0) + ">" + str(Threshold[index][0]) +" \n (:white_check_mark: S_INFO_WINDCODE_PREFIX >"+str(Threshold[index][1])+ ") OK!")
                elif int(tickers_0) < Threshold[index][0] and int(tickers_1) >= Threshold[index][1]:
                    msg_t.append(
                        ":warning:" + table + ": " + str(tickers_0) + " less than" + str(
                            Threshold[index][0]) + " \n( :white_check_mark: S_INFO_WINDCODE_PREFIX >" + str(
                            Threshold[index][1]) + ")")
                elif int(tickers_0) >= Threshold[index][0] and int(tickers_1) < Threshold[index][1]:
                    msg_t.append(
                        ":white_check_mark:" + table + ": " + str(tickers_0) + " > " + str(
                            Threshold[index][0]) + " \n ( :warning: S_INFO_WINDCODE_PREFIX less than" + str(
                            Threshold[index][1]) + ")")
                elif int(tickers_0) < Threshold[index][0] and int(tickers_1) < Threshold[index][1]:
                    msg_t.append(
                        ":warning:" + table + ": " + str(tickers_0) + " less than " + str(
                            Threshold[index][0]) + " \n ( :warning: S_INFO_WINDCODE_PREFIX less than" + str(
                            Threshold[index][1])+") ")
                    error_table.append(table)
                    error_thre.append(Threshold[index])
            else:
                if int(tickers_0) >= Threshold[index]:
                    msg_t.append(":white_check_mark:" + table + ": " + str(tickers_0) + ">" + str(Threshold[index]) + " OK!")
                else:
                    msg_t.append(":warning:" + table + ": " + str(tickers_0) + " less than  "+ str(Threshold[index]) + " ERROR!")
                    error_msg.append( table + ": " + str(tickers_0) + " less than  "+ str(Threshold[index]) + " ERROR!")
                    error_table.append(table)
                    error_thre.append(Threshold[index])
    conn.close()
    msg = {}
    url = WebhookUrl
    for index, table in enumerate(TABLES):
        msg.update({table: msg_t[index]})
        msg.update({'date': today_dt.strftime("%Y-%m-%d")})
    slack_msg = json.dumps(msg)
    slack_data = SendToSNS(json.loads(slack_msg),TABLES).slack_data()
    request = Request(
        url,
        data=json.dumps(slack_data).encode(),
        headers={'Content-Type': 'application/json'}
    )
    ssl._create_default_https_context = ssl._create_unverified_context
    response = urlopen(request)
    return error_msg, error_table,error_thre

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
    today_dt = datetime.datetime.now()
    #today_dt = datetime.date(2020,8,7)
    if check_tradedt(today_dt):
        errormsg,errortable, errorthre = selectFromDB(today_dt)
        if errormsg != []:
            sendAPI(errormsg)
        reviewError(errormsg, errortable)

    else:
        msg = {"date": today_dt.strftime("%Y-%m-%d")}
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



