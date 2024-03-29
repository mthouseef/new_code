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
from firebase_admin import firestore
from PIL import Image
import io
import hashlib 
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import requests


try:
    db.reference('/news/')
except:
    #cred = credentials.Certificate(os.getcwd()+'/cron/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    #cred = credentials.Certificate('/home/ubuntu/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    cred = credentials.Certificate('/home/dev/Downloads/quickakhbar20-firebase-adminsdk-6oacd-661156e85b.json')
    default_app = firebase_admin.initialize_app(cred,{'databaseURL': 'https://quickakhbar20.firebaseio.com','storageBucket':'quickakhbar20.appspot.com'})
    #default_storage_app = firebase_admin.initialize_app(cred,{'storageBucket':'https://quickakhbar20.appspot.com'})

bucket = storage.bucket("quickakhbar20.appspot.com")
size = 512,512


def get_page_content(link):
        agent = {"User-Agent":'Opera/9.80 (Linux armv7l; InettvBrowser/2.2 (00014A;SonyDTV140;0001;0001) KDL32W705B; CC/GBR) Presto/2.12.407 Version/12.50'}
        page = requests.get(link,headers=agent)
        tree = html.fromstring(page.content) if page else ""
        return tree

def parse_news(link,data):
    try:
        #import pdb;pdb.set_trace()
        stop_words = set(stopwords.words(data["language"]))
        db = firestore.client()
        transaction = db.transaction()
        ref = db.collection('news_v2'+'/'+data["country"])
        #@firestore.transactional()
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
        dictnry = {id_:{"title":head_lines,"domain":data["domain"],"url":link,"image_url":com_image,"date":source_date,
                    "time":published_time,"summary":summary,"live_date":live_date,"contry":data["country"],"key":key,"category":data["category"]}}
        country, lang = data["country"].split("/")
        site_map_dict = {"key":id_, "country":country,"language": lang}
        #users_ref = ref.child(''.join(filter(str.isalnum, id_)))
        #users_ref.set(dictnry)
        print("bbb")
        import pdb;pdb.set_trace()
        aaa = update_in_transaction(transaction, ref,dictnry)
        print("aaa")
        return dictnry,site_map_dict
    except:
        #logger.error("something in xpath happened", exc_info=True)
        return

@firestore.transactional
def update_in_transaction(transaction, news_ref,d):
    #import pdb; pdb.set_trace()
    news_to_write = [];
    import pdb; pdb.set_trace()
    for key,value in d.items():
        doc_ref = news_ref.document(key);
        try:
            snapshot = doc_ref.get(field_paths=[], transaction=transaction)
            if not snapshot.exists:
                news_to_write.append(key)
        except Exception as e:
            if(e.code == 404): 
                news_to_write.append(key)
    print(news_to_write)
    try:
        for key in news_to_write:
            doc_ref = news_ref.document(key)
            transaction.create(doc_ref, document_data=d[key])
        return True
    except Exception as e:
        print(e)
        return False


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

def GenerateSitemap(newsList):
    import pdb;pdb.set_trace()
    root = ET.Element('urlset', attrib={"xmlns":"http://www.sitemaps.org/schemas/sitemap/0.9"})
    date = datetime.now().strftime("%Y-%m-%d");
    # home page
    url = ET.SubElement(root, "url")
    addValue(url, 'loc', 'https://www.quickakhbar.com')
    addValue(url, "lastmod", "2021-04-01")
    addValue(url, "changefreq", "monthly")
    for news in newsList:
        url = ET.SubElement(root, "url")
        addValue(url, "loc", f'https://www.quickakhbar.com/{news["country"]}/{news["language"]}/{news["key"]}')
        addValue(url, "lastmod", date)
        addValue(url, "changefreq", "monthly")
        tree = ET.ElementTree(root)
        tree.write("./sitemap.xml")
        bucket = firebase_admin.storage.bucket()
        b = bucket.blob("sitemap.xml")
        b.upload_from_filename("./sitemap.xml")
        b.make_public()
        x = requests.get('https://www.quickakhbar.com/sitemap.xml')
                                                                                                    
def addValue(parent, name, value):
    child = ET.SubElement(parent, name)
    child.text = value
        
for data in news_data:
    array = []
    sm_array = []
    tree = get_page_content(data["url"])
    node = tree.xpath(data["news_url_xpath"]) if tree else ""
    url_array = [ data["domain"] + x if "http" not in x else x for x in list(set(node))]
    for i in url_array[0:3]:
        val,s_map = parse_news(i,data)
        if val != None :
            array.append(val)
            sm_array.append(s_map)
    GenerateSitemap(sm_array)
    #import pdb;pdb.set_trace()
    if not array:
        with open("/home/ubuntu/log.txt", 'a') as f:
            f.write("\n")
            f.write("***************************************************************")
            f.write("\n")
            f.write(json.dumps(data))
            f.write("\n")
            f.write("***************************************************************")
