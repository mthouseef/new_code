#from urllib.request import urlopen
#import urllib3
import requests
#from dask import delayed, compute
#import dask
import time
from datetime import *
from summa import summarizer
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import re
import pytz
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
from lxml import etree
import lxml.html
from lxml import html
from news_details_v2 import news_data
import uuid
from firebase_admin import storage
from datetime import datetime, timedelta
from PIL import Image
import io
import hashlib 
import json


try:
    db.reference('/news/')
except:
    cred = credentials.Certificate(os.getcwd()+'/cron/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    #cred = credentials.Certificate('/home/ubuntu/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    #cred = credentials.Certificate('/home/dev/Downloads/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    default_app = firebase_admin.initialize_app(cred,{'databaseURL': 'https://quickakhbar20.firebaseio.com'})

bucket = storage.bucket("quickakhbar20.appspot.com")
size = 512,512


def get_page_content(link):
        agent = {"User-Agent":'Opera/9.80 (Linux armv7l; InettvBrowser/2.2 (00014A;SonyDTV140;0001;0001) KDL32W705B; CC/GBR) Presto/2.12.407 Version/12.50'}
        page = requests.get(link,headers=agent)
        tree = html.fromstring(page.content) if page else ""
        return tree

def parse_news(link,data):
    try:
        import pdb;pdb.set_trace()
        stop_words = set(stopwords.words(data["language"]))
        ref = db.reference('/news_v2'+'/'+data["country"]+'/'+data["category"]+"/")
        tree = get_page_content(link)
        tz = pytz.timezone(data["time_zone"])
        
        source_date=""
        head_lines = tree.xpath(data["title_xpath"])[0].strip()
        image_source = tree.xpath(data["image_xpath"])[0]
        image = "https:" + image_source if "http" not in image_source else image_source
        live_date = datetime.utcnow().strftime('%Y-%m-%d %H')
        published_time=datetime.now(tz=tz).strftime('%H:%M')
        id_ = hashlib.md5(head_lines.encode()).hexdigest()
        #import pdb;pdb.set_trace()
        key = live_date + id_
        com_image = url_to_firebase(image, key)
        details= ' '.join(tree.xpath(data["description_xpath"])).strip() 
        summary = get_summary(details,stop_words)
        if summary == None:
            return
        dictnry={"title":head_lines,"domain":data["domain"],"url":link,"image_url":com_image,"date":source_date,
                    "time":published_time,"summary":summary,"live_date":live_date,"contry":data["country"],"key":key}
        users_ref = ref.child(''.join(filter(str.isalnum, id_)))
        users_ref.set(dictnry)
        return dictnry
    except:
        #logger.error("something in xpath happened", exc_info=True)
        return


def url_to_firebase(url, name):
    resp = requests.get(url, stream=True).raw
    agent = {"User-Agent":'Opera/9.80 (Linux armv7l; InettvBrowser/2.2 (00014A;SonyDTV140;0001;0001) KDL32W705B; CC/GBR) Presto/2.12.407 Version/12.50'}
    file_name = str(datetime.now().strftime("%Y-%m-%d")) + "/" + name
    resp = requests.get(url, headers=agent, stream=True)
    img = Image.open(io.BytesIO(resp.content))
    #img = Image.open(resp);
    img.thumbnail(size);
    inmemoryfile = io.BytesIO()
    img.save(inmemoryfile,format=img.format)
    blob = bucket.blob(file_name);
    blob.upload_from_string(inmemoryfile.getvalue())
    return blob.generate_signed_url(datetime.now() + timedelta(30))

def get_summary(details,stop_words):
    summary = summarizer.summarize(details, words=70)
    details=details.replace("\n","")
    if not summary:
        return
    elif len(summary)>500:
        summary=summary[:500]
        if "." not in summary[-1]:
            for k,c in enumerate(summary[::-1]):
                if "." in c:
                    summary=summary[:-k]
                    summary1=summary.split(" ")[0]
                    if summary1 in stop_words:
                        summary=summary[len(summary1):].strip()
                    else:
                        summary=summary.replace("\n"," ")
                        break
        else:
            summary1=summary.split(" ")[0]
            if summary1 in stop_words:
                summary=summary[len(summary1):].replace("\n"," ").strip()
            else:
                summary=summary.replace("\n"," ")
    return summary
        
for data in news_data:
    array = []
    tree = get_page_content(data["url"])
    #import pdb;pdb.set_trace()
    node = tree.xpath(data["news_url_xpath"]) if tree else ""
    url_array = [ data["domain"] + x if "http" not in x else x for x in list(set(node))]
    for i in url_array:
        val = parse_news(i,data)
        if val != None :
            array.append(val)
    if not array:
        with open("/home/ubuntu/log.txt", 'a') as f:
            f.write("\n")
            f.write("***************************************************************")
            f.write("\n")
            f.write(json.dumps(data))
            f.write("\n")
            f.write("***************************************************************")
