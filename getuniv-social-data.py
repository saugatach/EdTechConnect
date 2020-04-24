#!/usr/bin/env python3

import numpy as np
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup as bs
import time
import datetime as dt


def sleepytime(sleep=2):
    time.sleep(np.random.randint(3)+sleep)


def extractnumber(s):
    return re.findall('(\d*,?\d*,?\d*,?\d+,?\d+)', s)[0].replace(',', '')


def fbdata(fblink, datatoscrape, verbose=True):

    urlfb = 'https://www.facebook.com/' + fblink

    if verbose:
        print("Getting", urlfb)

    page = requests.get(urlfb, headers=headers)
    soup = bs(page.text, 'lxml')

    for el in soup.findAll('div', class_='_4bl9'):
        try:
            divtext = el.findAll('div')[0].text
            # print(divtext)
            if len(re.findall('like this', divtext)) > 0:
                datatoscrape['fblikes'] = extractnumber(divtext)
            if len(re.findall('follow this', divtext)) > 0:
                datatoscrape['fbfollows'] = extractnumber(divtext)
        except AttributeError:
            continue
    if any(datatoscrape):
        datatoscrape['date'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
    sleepytime()

    return datatoscrape


def wikidata(univ, datatoscrape, verbose=True):

    urlwiki = 'https://en.wikipedia.org/w/index.php?search="' + univ + '"'
    page = requests.get(urlwiki, headers=headers)
    if verbose:
        print("Getting", urlwiki)

    # try if the wiki page have the table of relevant info about the university, usually it does
    dfdata = pd.read_html(page.text)
    for key in ['Students', 'Academic staff', 'Administrative staff']:
        data1 = dfdata[(dfdata[0] == key)][1]
        if len(data1) > 0:
            extractnum = data1.str.extract('(\d*,?\d*,?\d*,?\d+,?\d+)')[0].iloc[0]
            try:  # it is not np.nan which codes as float
                dataval = extractnumber(extractnum)
            except:
                dataval = data1.values[0]
        else:
            dataval = np.nan
        datatoscrape[key] = dataval

    data1 = dfdata[(dfdata[0] == 'Type')][1]
    dataval = re.findall('(Private|private|Public|public).*', str(data1.values))
    if len(dataval) > 0:
        datatoscrape['Type'] = dataval[0]
    else:
        datatoscrape['Type'] = ''

    data1 = dfdata[(dfdata[0] == 'Website')][1]
    datatoscrape['Website'] = data1.values[0]
    return datatoscrape


def getdata(df, csvfile='univ_data.csv', verbose=True):

    if 'date' not in df.columns:
        df['date'] = ''
        df_50 = df[:50]
    else:
        natvalues = list(df['date'].map(lambda x: pd.isna(x)))
        dftemp = df[natvalues]

        emptyrows = len(dftemp)
        if emptyrows > 50:
            df_50 = dftemp[:50]
        elif emptyrows > 0:
            df_50 = dftemp
        else:
            df_50 = df.sort_values(by='date', ascending=True)[:50]   #TODO: the sorting of date not working

    datatoscrape_wiki = {'Students': '', 'Academic staff': '', 'Administrative staff': '', 'Type': '', 'Website': ''}
    datatoscrape_fb = {'fblikes': '', 'fbfollows': '', 'date': ''}

    for k in datatoscrape_wiki:
        if k not in df.columns:
            df[k] = ''
    for k in datatoscrape_fb:
        if k not in df.columns:
            df[k] = ''

    for inx, univ, fblink in zip(df_50.index, df_50['Name'], df_50['fblink']):

        datatoscrape_wiki = wikidata(univ, datatoscrape_wiki, verbose)
        if any(datatoscrape_wiki):
            df_50 = assignvalues(df_50, inx, datatoscrape_wiki)

        datatoscrape_fb = fbdata(fblink, datatoscrape_fb, verbose)
        if any(datatoscrape_fb):
            df_50 = assignvalues(df_50, inx, datatoscrape_fb)
        df.update(df_50)
        df.to_csv(csvfile, index=False)

    return df


def assignvalues(df, inx, datadict):

    for key in datadict:
        if key not in df.columns:
            df[key] = ''

    # fill the relevant columns of df with data
    for key, value in datadict.items():
        df[key].loc[inx] = value

    return df


# ---========================MAIN MODULE=========================---

df_univ = pd.read_csv('univ_data.csv')
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

getdata(df_univ, verbose=True)
