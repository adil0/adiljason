import requests
import urllib
from bs4 import BeautifulSoup
import re
import unicodedata
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import smtplib
import feedparser
import xml.etree.ElementTree as ET
import time
import io
import math
import json
from pandas.tseries.offsets import BDay
pd.set_option('display.max_colwidth', -1)

# load the ticker data and create a data frame
ticker_url = 'https://www.sec.gov/include/ticker.txt'

# request that new content, will be JSON STRUCTURE!
content = requests.get(ticker_url).content
data = content.decode("utf-8").split('\n')

###get stock information(from yahoo finance)
def get_price_vol_desc(ticker):
    print(ticker)
    d = {}
    try:
        if type(ticker)== float:
            return d
        elif ticker != '':
            res = requests.get('http://finance.yahoo.com/q?s=' + ticker)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # get the summary stats table
            script = soup.find('script', text=re.compile('root\.App\.main'))
            json_text = re.search(r'^\s*root\.App\.main\s*=\s*({.*?})\s*;\s*$',
                          script.string, flags=re.MULTILINE).group(1)
            data = json.loads(json_text)
            
            d['ticker']         = ticker
            d['Price']          = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['regularMarketPrice']['raw'])
            d['Prev_Close']     = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['regularMarketPreviousClose']['raw'])
            d['Open']           = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['regularMarketOpen']['raw'])
            d['Volume']         = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryDetail']['regularMarketVolume']['raw'])
            d['Avg_Volume']     = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryDetail']['averageVolume']['raw'])            
            d['Mkt_Cap']        = data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['marketCap']['fmt']
            d['description']    = data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryProfile']['longBusinessSummary']
            d['SharesOut']      = float(data['context']['dispatcher']['stores']['QuoteSummaryStore']['defaultKeyStatistics']['sharesOutstanding']['raw'])
            d['Return']         = round(100*(d['Price']/d['Prev_Close']-1),2)
            d['Gap_Return']     = round(100*(d['Open']/d['Prev_Close']-1),2)
            d['Day_Return']     = round(100*(d['Price']/d['Open']-1),2)
            d['Vol_Surprise']   = round((d['Volume']/d['Avg_Volume']),2)
            d['mktCapChng']     = round(d['SharesOut']*d['Price']*d['Return']/100) 
            return d
    except:
        return d

### basic stock information   
def get_description(ticker):
    d = {}
    try:
        res = requests.get('http://finance.yahoo.com/q?s=' + ticker)
        soup = BeautifulSoup(res.text, 'html.parser')
        script = soup.find('script', text=re.compile('root\.App\.main'))
        json_text = re.search(r'^\s*root\.App\.main\s*=\s*({.*?})\s*;\s*$',
                          script.string, flags=re.MULTILINE).group(1)
        data = json.loads(json_text)
        d['ticker']         = ticker
        d['Mkt_Cap']        = data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['marketCap']['fmt']
        d['description']    = data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryProfile']['longBusinessSummary']
        return d
    except:
        return d

### extract pre-market data from yahoo finance    
def get_preMktData(ticker):
    d = {}
    try:
        res = requests.get('http://finance.yahoo.com/q?s=' + ticker)
        soup = BeautifulSoup(res.text, 'html.parser')
        script = soup.find('script', text=re.compile('root\.App\.main'))
        json_text = re.search(r'^\s*root\.App\.main\s*=\s*({.*?})\s*;\s*$',
                            script.string, flags=re.MULTILINE).group(1)
        data = json.loads(json_text)
        d['ticker']         = ticker
        d['preMktRt']       = data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['preMarketChangePercent']['fmt']
        d['Mkt_Cap']        = data['context']['dispatcher']['stores']['QuoteSummaryStore']['price']['marketCap']['fmt']
        d['description']    = data['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryProfile']['longBusinessSummary']
        return d
    except:
        return d

### basic filing data
def scrape_txt(link):
    pdict = {}
    response = requests.get(link)
    # pass it through the parser, in this case let's just use lxml because the tags seem to follow xml.
    soup = BeautifulSoup(response.content, 'lxml')
               
    sec_header_tag = soup.find('sec-header')
    ## get the date time and company data from header
    header_list = list(filter(None, str(sec_header_tag).replace('\t','').split("\n")))
    item_info_indices = []
    for index, el in enumerate(header_list):
        if 'CENTRAL INDEX KEY:' in el:
            cik_index = index     
        elif 'FILER:' in el:
            sic_index = index + 4  
                        
    ## get the cik and industry classification data
    pdict['cik']                 = header_list[cik_index].split(":")[1]
    pdict['industry_class']      = header_list[sic_index].split(":")[1]    
    
    return pdict

### parse each filing
def edgar_feed(url, filing_types_filter, industry_class_filter):
    more_data_to_parse = True
    stocks_parsed = [] 
    d = feedparser.parse(url)
    last_filing_date = datetime.strptime(d.entries[99].updated.split('T')[0], '%Y-%m-%d').date()
    # if the last filing in the batch is from y'day, we ll stop after this batch
    if last_filing_date < (datetime.today() - BDay(1)).date(): 
        more_data_to_parse = False
    for entry in range(0,99):
        filing_time = datetime.strptime(d.entries[entry].updated.split('-04:00')[0].replace('T',' '), '%Y-%m-%d %H:%M:%S')
        # date time filter in the current batch
        if filing_time < datetime.combine((datetime.today() - BDay(1)).date(), dt.time(16, 0, 0)):
            break
        company_name = d.entries[entry].title
        company_name = company_name.split('- ')
        company_name = company_name[1].split(' (')
        company_name = company_name[0]
        company_name = company_name.replace('.', '')
        company_name = company_name.replace(',', '')
        if '&amp;' in company_name:
            company_name = company_name.replace('&amp;', '&')
        if not company_name in stocks_parsed:
            stocks_parsed.append(company_name) 
            filing_det = d.entries[entry].summary.split('<br>')[1:]
            filing_type_ids = []
            filing_types = []
            for i in range(len(filing_det)):
                sp_list = filing_det[i].split(":")
                filing_type_ids.append(sp_list[0])
                filing_types.append(sp_list[1].replace('\n','').strip())        

            filing_type_ids_str = ';'.join(map(str, filing_type_ids))
            filing_types_str    = ';'.join(map(str, filing_types))

            # parse the filing to get the CIK, SIC
            link = d.entries[entry].link.replace('-index.htm','.txt')
            sdict = scrape_txt(link)

            # store the final parsed data 
            parsed_dict = {}
            parsed_dict['company_name']        = company_name
            parsed_dict['filing_url']          = d.entries[entry].link
            parsed_dict['filing_datetime']     = filing_time
            parsed_dict['filing_type_id']      = filing_type_ids_str
            parsed_dict['filing_type']         = filing_types_str
            parsed_dict['cik']                 = sdict['cik']
            parsed_dict['industry_class']      = sdict['industry_class']
            parsed_dict['filing_id']           = d.entries[entry].id.split(":")[-1].split("=")[-1]        
            
            # now check the conditions to append the data to the master list
            if set(filing_type_ids).intersection(set(filing_types_filter)) and parsed_dict['industry_class'] in industry_class_filter:
                print(company_name)
                master_list.append(parsed_dict)
        else:
            pass
    return more_data_to_parse     


def main():
    ticker_list = []
    for item in data:
        ticker_dict = {}
        ticker_dict['ticker'] = item.split('\t')[0]
        ticker_dict['cik'] = item.split('\t')[1]
        ticker_list.append(ticker_dict)

    ticker_df = pd.DataFrame(ticker_list) 
    ticker_df['cik'] = ticker_df['cik'].astype('int')

    master_list = []
    filing_types_filter = ['Item 8.01', 'Item 7.01', 'Item 2.02']
    industry_class_filter = ['PHARMACEUTICAL PREPARATIONS [2834]', 'SERVICES-COMMERCIAL PHYSICAL &amp; BIOLOGICAL RESEARCH [8731]',
                             'BIOLOGICAL PRODUCTS (NO DIAGNOSTIC SUBSTANCES) [2836]', 'SURGICAL &amp; MEDICAL INSTRUMENTS &amp; APPARATUS [3841]',
                             'MEDICINAL CHEMICALS & BOTANICAL PRODUCTS [2833]']

    start = 0 
    more_data_to_parse = True
    while more_data_to_parse:
        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-k&company=&dateb=&owner=include&start='+str(start)+'&count=100&output=atom'
        more_data_to_parse = edgar_feed(url, filing_types_filter, industry_class_filter)
        if more_data_to_parse:
            start = start+100
        else:
            break

    df = pd.DataFrame(master_list)
    df = df.drop_duplicates(subset='filing_id', keep='first')
    #df['filing_datetime'] = df['filing_datetime'].apply(lambda x: datetime.strptime(x, '%Y%m%dT%H%M%S'))
    df['cik'] = df['cik'].astype('int')
    df = df.merge(ticker_df, how='left')
    df = df.drop_duplicates(subset='filing_id', keep='first')
    df= df.sort_values(by=['filing_datetime'])
    df = df[['company_name', 'filing_datetime', 'ticker', 'filing_url', 'filing_type','industry_class',
            'cik', 'filing_id']]    
    date= str(datetime.today().date()).replace("-","")
    pdate = str((datetime.today() - BDay(1)).date()).replace("-","")
    file_name = 'RT8K'+date+'.csv'
    pd_file_name = 'RT8K'+pdate+'.csv'

    if datetime.today().time() > dt.time(17, 30, 0):
        # remove the data from y'day file to avoid duplicates
        pd_df = pd.read_csv(pd_file_name)
        cut_off = datetime.combine((datetime.today() - BDay(1)).date(), dt.time(16, 0, 0))
        #update y'day's file
        pdd_df= pd_df[pd_df.filing_datetime.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))<cut_off]
        pdd_df.to_csv(pd_file_name, index=False)
        # update today's file
        d = df.ticker.apply(get_price_vol_desc)
        p_df = pd.DataFrame([d[1] for d in d.items()])
        df = df.merge(p_df)
        df = df[['company_name', 'filing_datetime', 'ticker', 'filing_url', 'filing_type','industry_class',
                'cik', 'filing_id','Vol_Surprise', 'Return', 'Gap_Return', 'Day_Return','Mkt_Cap','mktCapChng',
                'Price', 'Volume','Avg_Volume','Prev_Close','description']]        
        df.drop_duplicates(subset='filing_id', keep='first', inplace=True)
        df.to_csv(file_name, index=False)
    else:
        d = df.ticker.apply(get_preMktData)
        p_df = pd.DataFrame([d[1] for d in d.items()])
        df = df.merge(p_df)
        df = df[['company_name', 'filing_datetime', 'ticker', 'filing_url', 'filing_type','industry_class',
                'cik', 'filing_id','preMktRt','Mkt_Cap','description']]  
        df.drop_duplicates(subset='filing_id', keep='first', inplace=True)
        df.to_csv(file_name, index=False)   

    ## save the data to csv file
    # remove the data from y'day file to avoid duplicates
    pd_df = pd.read_csv(pd_file_name)
    cut_off = datetime.combine((datetime.today() - BDay(2)).date(), dt.time(16, 0, 0))
    #update y'day's file
    pdd_df= pd_df[pd_df.filing_datetime.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))<cut_off]
    pdd_df.to_csv(pd_file_name, index=False)
    # update today's file
    d = df.ticker.apply(get_price_vol_desc)
    p_df = pd.DataFrame([d[1] for d in d.items()])
    df = df.merge(p_df)
    df = df[['company_name', 'filing_datetime', 'ticker', 'filing_url', 'filing_type','industry_class',
                'cik', 'filing_id','Vol_Surprise', 'Return', 'Gap_Return', 'Day_Return','Mkt_Cap','mktCapChng',
                'Price', 'Volume','Avg_Volume','Prev_Close','description']]        
    df.drop_duplicates(subset='filing_id', keep='first', inplace=True)
    df.to_csv(file_name, index=False)    

if __name__ == '__main__':
    main()    