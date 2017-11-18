from __future__ import print_function
import bs4
import requests
#import pandas as pd
import traceback
import datetime
from multiprocessing import Pool
#from pandas.io import sql
from sqlalchemy import create_engine, MetaData, Table, insert
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse


engine = create_engine('mysql+mysqldb://jgorman:jacK18gOrman@127.0.0.1/jgorman')
session = sessionmaker(bind=engine)
s = session()
metadata = MetaData(bind=engine)
mytable = Table('armslist_test',metadata, autoload=True)

url1 = 'http://www.armslist.com/classifieds/search?location=usa&category=guns&page='
url2 = '&posttype=7&ships=True'
column_names = ['title', 'price', 'party_type', 'register_date', 'register_status', 'location', 'ships',
                'category', 'manufacturer','caliber', 'action', 'gun_type', 'body', 'listing_date', 'post_id']

def get_product_urls(soup):
    productDivs = soup.findAll('div', attrs={'style' : 'position: relative; clear: both;'})
    products = ["http://www.armslist.com" + div.find('a')['href'] for div in productDivs]
    return products #list of each posting

def get_product_data(item):
    try:
        r_temp = requests.get(item)
        data = bs4.BeautifulSoup(r_temp.content, "html.parser")
        title = data.find('div', attrs={'class' : 'col-md-6 col-sm-8'}).h1.text.encode('utf-8','ignore')
        try:
            price = data.find('span', attrs={'class' : 'price'}).text.strip()
            price = int(price.strip().replace('$','').replace(',',''))
        except:
            price = None
        party_type = [i.get_text().strip().split() for i in data.findAll('strong', attrs={'class' : 'title'})]
        party_type = str(party_type[0][0]) +' '+ str(party_type[0][1])
        #import pdb;pdb.set_trace()
        try:
            register_date_unformatted = data.find('time').text.replace('Registered on ','')
            register_date_parsed = parse(register_date_unformatted)
            register_date = register_date_parsed.strftime('%Y-%m-%d')
        except:
            register_date = None

        if data.find('div', attrs={'class' : 'alert alert-danger'}) is not None:
                register_status = "unregistered"
        elif data.find('div', attrs={'class' : 'alert alert-warning'}) is not None:
            register_status = "unconfirmed"
        else:
            register_status = "registered"

        ltemp = data.findAll('ul', attrs={'class' : 'location'})
        location_class = [i.get_text().strip() for i in data.findAll('div', attrs={'class' : 'col-sm-12 col-md-7'})]
        if len(ltemp)==2:
            location = location_class[2].encode('utf-8','ignore')
            ships = location_class[3]
        else:
            location = location_class[0].encode('utf-8','ignore')
            ships = location_class[1]

        info = data.find('div', attrs={'class' : "info-holder" }).find('ul', attrs={'class' : "category" }).findAll('li')

        labels = ['MANUFACTURER', 'CATEGORY','CALIBER','ACTION','FIREARM TYPE']

        d = {}

        for p in range(len(info)):
            c = info[p].findAll('span')
            d[c[0].get_text().strip().upper()] = [c[1].get_text().strip()]
        if labels[0] in d:
            manufacturer = d['MANUFACTURER'][0]
        else:
            manufacturer = "NA"
        if labels[1] in d:
            category = d['CATEGORY'][0]
        else:
            category = "NA"
        if labels[2] in d:
            caliber = d['CALIBER'][0]
        else:
            caliber = "NA"
        if labels[3] in d:
            action = d['ACTION'][0]
        else:
            action = "NA"
        if labels[4] in d:
            gun_type = d['FIREARM TYPE'][0]
        else:
            gun_type = "NA"

        body = data.find('div', attrs={'class' : "text-holder" }).get_text().strip().encode('utf-8','ignore')
        listing_date_unformatted = data.find('div', attrs={'class' : "info-time" }).find('span', attrs = {'class' : 'date'}).get_text()[11:]
        listing_date_parsed = parse(listing_date_unformatted)
        listing_date = listing_date_parsed.strftime('%Y-%m-%d')
        post_id = int(data.find('div', attrs={'class' : "info-time" }).find('span', attrs = {'class' : 'user-id'}).get_text()[9:].strip())

        row_list = [title, price, party_type, register_date, register_status, location, ships,
                    category, manufacturer, caliber, action, gun_type, body, listing_date, post_id]
            
        i = insert(mytable)
        i = i.values({'title':title,'price':price,'party_type':party_type,'register_date':register_date, 'register_status':register_status,'location':location,'ships':ships,'category':category,'manufacturer':manufacturer,'caliber':caliber,'action':action,'gun_type':gun_type,'body':body,'listing_date':listing_date,'post_id':post_id})
        s.execute(i)

    except:
        traceback.print_exc()
        print(item)
        import pdb;pdb.set_trace()

    #return row_list


if __name__ == "__main__":
    p = Pool(5)
    page = 1
    all_page_data = []
    while page < 3:
        r = requests.get(url1 + str(page) + url2) #returns the HTML of the page, can be done through urlopen as well
        soup = bs4.BeautifulSoup(r.content, "html.parser")
        #print(soup.find('div', attrs={'class' : 'col-xs-12 col-md-8'}))
        if soup.find('div', attrs={'class' : 'col-xs-12 col-md-8'}).text != "No active listings.":
            products = get_product_urls(soup)
            #get_product_data(products[0])
            data_list = p.map(get_product_data, products)
            # data_list = get_product_data(products)
            all_page_data += data_list
            page += 1
        else:
            break
    p.terminate()
    p.join()
    # import pdb;pdb.set_trace()
    #df = pd.DataFrame(all_page_data, columns = column_names)

    # print(df)
    #df.to_csv("output.csv", columns = column_names, header=True)
    #df.to_sql(name='armslist_test', con=engine, if_exists = 'append', index=False)


# PYTHON2_VERSION=2.7.8
# export PATH=/home/twitter-data/applications/python-$PYTHON2_VERSION/bin:$PATH

# export WORKON_HOME=$HOME/.virtualenvs
# export VIRTUALENVWRAPPER_PYTHON=$(which python)
# source /home/twitter-data/applications/python-$PYTHON2_VERSION/bin/virtualenvwrapper.sh



