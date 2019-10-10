#!/usr/bin/env python
# coding: utf-8

import argparse
import json
import time
import pymysql
import re
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession

args_list = ["config_file", "keyword", "request_url", "stage", "param"]


def user_input():
    config = argparse.ArgumentParser()
    config.add_argument('-cf', '--config_file', help='config file name', default='', type=str, required=False)
    config_file_check = config.parse_known_args()
    object_check = vars(config_file_check[0])

    records = []
    if object_check['config_file'] != '':
        json_file = json.load(open(config_file_check[0].config_file))
        db_url = json_file['db_url']
        for record in range(0, len(json_file['Records'])):
            arguments = {}
            for i in args_list:
                arguments[i] = None
            for key, value in json_file['Records'][record].items():
                arguments[key] = value
            records.append(arguments)
    else:
        # Taking command line arguments from users
        parser = argparse.ArgumentParser()
        parser.add_argument('-db', '--db_url', help='db_url', type=str, required=True)
        parser.add_argument('-k', '--keyword', help='keyword', type=str, required=True)
        parser.add_argument('-ru', '--request_url',
                            help='prefer to type request url in db', type=str, required=True)
        parser.add_argument('-st', '--stage',
                            help='1. search result list of keyword\
                             2. select one of them and catch it', type=str, required=True)
        parser.add_argument('-p', '--param', help='parameter which name or code', type=str, required=True)


        parser.add_argument('-gd', '--get_data', help='just get data from site', type=bool, nargs='?',
                            const=True, required=False)

        args = parser.parse_args()
        arguments = vars(args)
        records.append(arguments)
        db_url = records[0].pop('db_url')
    return db_url, records


class CrawlerWithDb:
    def __init__(self):
        pass

    def get_rules_from_db(self, db_url, arguments):
        rules = dict()
        user, password, host, port, database = re.match('mysql://(.*?):(.*?)@(.*?):(.*?)/(.*)', db_url.lower()).groups()
        db = pymysql.connect(host=host, port=int(port), user=user, passwd=password, db=database, charset='utf8')

        try:
            with db.cursor() as cursor:
                sql = "SELECT * FROM sources WHERE code = %s and stage = %s and parameter = %s"
                cursor.execute(sql, [arguments['request_url'], arguments['stage'], arguments['param']])
                _, _, rules['param'], rules['request_url'], rules['method'], rules['css_path'], rules['form_data'],\
                    rules['result_list_param'] , rules['result_code_param'], rules['result_name_param'], \
                    rules['result_total_page_param'], rules['result_current_page_param'] = cursor.fetchone()
        finally:
            db.close()

        return rules

    def parse_rules(self, crawl_rules, keyword, param_for_parse):
        crawl_rules[param_for_parse] = crawl_rules[param_for_parse].replace('[' + crawl_rules['param'].upper() + ']',
                                                                        keyword)
        crawl_rules[param_for_parse] = crawl_rules[param_for_parse].replace('[YEAR]', str(time.localtime().tm_year))
        crawl_rules[param_for_parse] = crawl_rules[param_for_parse].replace('[MONTH]', str(time.localtime().tm_mon))
        crawl_rules[param_for_parse] = crawl_rules[param_for_parse].replace('[DAY]', str(time.localtime().tm_mday))

        return crawl_rules

    def get_get_data(self, crawl_rules, arguments):
        crawl_rules = self.parse_rules(crawl_rules, arguments['keyword'], 'request_url')

        sess = HTMLSession()

        res = sess.get(crawl_rules['request_url'])
        res.html.render()

        soup = BeautifulSoup(res.html.html, 'lxml')

        if arguments['stage'] == "select":
            # result_data = re.sub("[^\d\.%]", "", soup.select(crawl_rules['css_path'])[0].text)
            result_data = soup.select(crawl_rules['css_path'])[0].text
            test = soup.select(crawl_rules['css_path'])
        else:
            result_data = soup.select(crawl_rules['css_path'])[0].text

        return result_data

    def get_post_data(self, crawl_rules, arguments):
        crawl_rules = self.parse_rules(crawl_rules, arguments['keyword'], 'form_data')

        crawl_rules['form_data'] = json.loads(crawl_rules['form_data'])

        res = requests.post(crawl_rules['request_url'], data=crawl_rules['form_data'])

        content = json.loads(res.text)

        if crawl_rules['result_list_param'] is not None:
            last_page = int(content[crawl_rules['result_list_param']][0][crawl_rules['result_total_page_param']])
            result_data = []
            for line in content[crawl_rules['result_list_param']]:
                result_data.append(
                    str(line[crawl_rules['result_code_param']]) + " " + str(line[crawl_rules['result_name_param']]))
            for i in range(2, last_page+1):
                crawl_rules['form_data'][crawl_rules['result_current_page_param']] = i
                res = requests.post(crawl_rules['request_url'], data=crawl_rules['form_data'])
                if type(res.content) is bytes:
                    content = json.loads(res.content.decode('utf8'))

                for line in content[crawl_rules['result_list_param']]:
                    result_data.append(
                        str(line[crawl_rules['result_code_param']]) + " " + str(line[crawl_rules['result_name_param']]))

        return result_data

    def get_data_from_web(self, db_url, arguments):
        if __name__ != "__main__":
        # TODO: if the calling file contains config_file param
            if 'config_file' in arguments:
                records = []
                json_file = json.load(open(arguments['config_file']))
                for record in range(0, len(json_file['Records'])):
                    arguments = {}
                    for i in args_list:
                        arguments[i] = None
                    for key, value in json_file['Records'][record].items():
                        arguments[key] = value
                    records.append(arguments)
                total_errors = 0
                for rec in records:
                    # paths, errors = self.download_executor(rec)
                    # for i in paths:
                        # paths_agg[i] = paths[i]
                    if not arguments["silent_mode"]:
                        if arguments['print_paths']:
                            print('a')
                            # print(paths.encode('raw_unicode_escape').decode('utf-8'))
                    # total_errors = total_errors + errors
                # return paths_agg,total_errors
            else:
                crawl_rules = self.get_rules_from_db(db_url, arguments)
        else:
            crawl_rules = self.get_rules_from_db(db_url, arguments)
            if crawl_rules['method'] == "get":
                result_data = self.get_get_data(crawl_rules, arguments)
            elif crawl_rules['method'] == "post":
                result_data = self.get_post_data(crawl_rules, arguments)

        return result_data


def main():
    db_url, records = user_input()
    t0 = time.time()  # start the timer

    for arguments in records:
        crawler = CrawlerWithDb()
        result_data = crawler.get_data_from_web(db_url, arguments)
        print(result_data)
        t1 = time.time()  # stop the timer
        total_time = t1 - t0

        print("\nEverything Finished!")
        print("Total time taken: " + str(round(total_time, 2)) + " Seconds")


if __name__ == "__main__":
    main()