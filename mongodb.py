#coding:utf-8
'''
for test
'''

import pymongo
import random
from pymongo import MongoClient
from model.Item import PageContent
import random
import sys  
sys.path.append("..") 
from util import utcTime
from store import configureRead
import cgi
import time
from datetime import date,datetime
from model import Item
import logging.config
import codecs
from datetime import timedelta
import yaml

logger = logging.getLogger("commonsparser")

def save(pages=[],time_type='MDY_FORMAT'):
    itemsLen=len(pages)
    successLen=0

    logging.info('start save size :' + str(len(pages)))
    #client = MongoClient(configureRead.getDBValue('mongodb', 'db_host'), int(configureRead.getDBValue('mongodb', 'db_port')))      
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))      
     #client = MongoClient('mongodb://localhost:27017/')
   
#db = client['test-database']
#collection = db['test-collection']
    db = client[configureRead.getDBValue('mongodb', 'db_name')]
    
    table_name=configureRead.getDBValue('mongodb', 'db_table_newsInfo')
    #db.authenticate(configureRead.getDBValue('mongodb', 'db_user'),configureRead.getDBValue('mongodb', 'db_pass')) 
    logging.info('save size :'+str(len(pages)))
    tmpItem=[]
    for page in pages:
        try:    
            #db.NewsInfo.insert({'id':item.id,'title':cgi.escape(item.Name),'content':cgi.escape(item.content),'priority':item.Priority,'publishTime':item.publishTime,'retriveTime':utcTime.getCurrentTime(),'category':item.Subject,'fetchURL':cgi.escape(item.URL),'imagePath':cgi.escape(item.ImagePath)}) 
            if(not page.image_path):
                page.Priority=1
            if(not page.time_type):
                db[table_name].insert({'_id':str(page.id),
                                'title':cgi.escape(page.Name),
                                'digest':cgi.escape(page.digest),
                                'priority':page.Priority,
                                'newsDate':utcTime.converter(page.PublishTime,time_type),
                                'fetchDate':utcTime.getCurrentTime(),
                                'category':page.Subject,
                                'url':cgi.escape(page.URL),#设置unit key
                                'imagePaths':page.image_path,
                                'loc': page.loc,
                                'cc': page.cc,
                                'shardkey': int(page.shardkey),
                                'siteURL': page.SiteURL,
                                'inBlackList': 0})
            else:
                db[table_name].insert({'_id':str(page.id),
                                'title':cgi.escape(page.Name),
                                'digest': cgi.escape(page.digest),
                                'priority':page.Priority,
                                'newsDate':utcTime.converter(page.PublishTime,page.time_type),
                                'fetchDate':utcTime.getCurrentTime(),
                                'category':page.Subject,
                                'url':cgi.escape(page.URL),#设置unit key
                                'imagePaths':page.image_path,
                                'loc': page.loc,
                                'cc': page.cc,
                                'shardkey':int(page.shardkey),
                                'siteURL':page.SiteURL,
                                'inBlackList': 0})
            tmpItem.append((page.JobName,page.URL,utcTime.getCurrentTime4UTC(),page.id,page.Subject,page.SiteURL))
            successLen+=1
        except AttributeError as e:
            url=''          
            if(page):
                url=page.URL
            logging.error(str(e.message)+" URL :"+url)
                     
            continue
        except Exception as e:
            url=''          
            if(page):
                url=page.URL
            logging.error(str(e.message)+" URL :"+url)
            continue
    client.close()
    return (itemsLen,successLen)


def saveThumbnail(pages=[], time_type='UTC'):
    itemsLen = len(pages)
    successLen = 0
    # client = MongoClient(configureRead.getDBValue('mongodb', 'db_host'), int(configureRead.getDBValue('mongodb', 'db_port')))
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))
    # client = MongoClient('mongodb://localhost:27017/')

    # db = client['test-database']
    # collection = db['test-collection']
    db = client[configureRead.getDBValue('mongodb', 'db_name')]

    table_name = configureRead.getDBValue('mongodb', 'db_table_newsthumbnail')
    # db.authenticate(configureRead.getDBValue('mongodb', 'db_user'),configureRead.getDBValue('mongodb', 'db_pass'))
    logger.info('save size :' + str(len(pages)))
    for page in pages:

        try:
            db[table_name].update({'_id': str(page.URL)},
                                  {"$set":{'imageUrl': page.image_path,
                                       'digest': cgi.escape(page.digest),
                                       'title': cgi.escape(page.Name),
                                       'type' : 1,
                                       'siteUrl': cgi.escape(page.SiteURL),
                                       'fetchDate': utcTime.getCurrentTime()}},False,True)
            successLen += 1
        except AttributeError as error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " saveThumbnail :" + url)

            continue
        except error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " saveThumbnail :" + url)
            continue
    client.close()
    return (itemsLen, successLen)





def update_not_handled_Thumbnail(pages=[], time_type='UTC'):
    itemsLen = len(pages)
    successLen = 0
    # client = MongoClient(configureRead.getDBValue('mongodb', 'db_host'), int(configureRead.getDBValue('mongodb', 'db_port')))
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))
    # client = MongoClient('mongodb://localhost:27017/')

    # db = client['test-database']
    # collection = db['test-collection']
    db = client[configureRead.getDBValue('mongodb', 'db_name')]

    table_name = configureRead.getDBValue('mongodb', 'db_table_newsthumbnail')
    # db.authenticate(configureRead.getDBValue('mongodb', 'db_user'),configureRead.getDBValue('mongodb', 'db_pass'))
    logger.info('save size :' + str(len(pages)))
    for page in pages:

        try:
            db[table_name].update({'_id': str(page.URL)},
                                  {"$set":{'type' : 2}},False,True)
            successLen += 1
        except AttributeError as error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " update_not_handled_Thumbnail :" + url)

            continue
        except error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " update_not_handled_Thumbnail :" + url)
            continue
    client.close()
    return (itemsLen, successLen)
    
def find_newsthumbnail():
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))
    db = client[configureRead.getDBValue('mongodb', 'db_name')]
    table_name = configureRead.getDBValue('mongodb', 'db_table_newsthumbnail')
    start = datetime.utcnow()-timedelta(days=1)
    contents = db[table_name].find({'imageUrl': None,'type':1, 'createDate':{'$gte': start}})
    items = []
    for content in contents:
        item = Item.Item()
        item.URL = content.get("_id")
        items.append(item)
    return items


def saveSiteThumbnail(pages=[], time_type='UTC'):
    itemsLen = len(pages)
    successLen = 0
    # client = MongoClient(configureRead.getDBValue('mongodb', 'db_host'), int(configureRead.getDBValue('mongodb', 'db_port')))
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))
    # client = MongoClient('mongodb://localhost:27017/')

    # db = client['test-database']
    # collection = db['test-collection']
    db = client[configureRead.getDBValue('mongodb', 'db_name')]

    table_name = configureRead.getDBValue('mongodb', 'db_table_newsthumbnail')
    # db.authenticate(configureRead.getDBValue('mongodb', 'db_user'),configureRead.getDBValue('mongodb', 'db_pass'))
    logger.info('save size :' + str(len(pages)))
    for page in pages:

        try:
            db[table_name].save({ '_id': str(page.URL),
                                  'imageUrl': page.image_path,
                                       'digest': cgi.escape(page.digest),
                                       'title': cgi.escape(page.Name),
                                       'type' : 0,
                                       'siteUrl': cgi.escape(page.SiteURL),
                                       'fetchDate': utcTime.getCurrentTime(),
                                       'shardkey':int(page.shardkey)})
            successLen += 1
        except AttributeError as error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " saveThumbnail :" + url)

            continue
        except error:
            url = ''
            if (page):
                url = page.URL
            logger.error(str(error) + " saveThumbnail :" + url)
            continue
    client.close()
    return (itemsLen, successLen)



def savenumbers(numbers=[]):
    itemsLen = len(numbers)
    successLen = 0
    # client = MongoClient(configureRead.getDBValue('mongodb', 'db_host'), int(configureRead.getDBValue('mongodb', 'db_port')))
    client = MongoClient(configureRead.getDBValue('mongodb', 'db_url'))
    # client = MongoClient('mongodb://localhost:27017/')

    # db = client['test-database']
    # collection = db['test-collection']
    db = client[configureRead.getDBValue('mongodb', 'db_name')]

    table_name = 'UserWhiteList'

    for number in numbers:

        try:
            db[table_name].save({ '_id': number,
                                       'valid' : 1})
            successLen += 1
        except AttributeError as error:
            logger.error(str(error) + " savenumbers :")

            continue
        except error:
            logger.error(str(error) + " savenumbers :")
            continue
    client.close()
    return (itemsLen, successLen)

