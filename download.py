import pandas as _pd
import requests as _requests
from multiprocessing.pool import ThreadPool as _ThreadPool
from time import sleep as _sleep
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from pandas.tseries.holiday import USFederalHolidayCalendar as _calendar
import tqdm as _tqdm
import re as _re


def download(start=None, end=None, form='4'):
    """Download insider trading data from SEC
    :Parameters:
        start: str
            Download start date string (YYYY-MM-DD) or datetime.
            Default is 1 days ago
        end: str
            Download end date string (YYYY-MM-DD) or datetime.
            Default is yesterday
        form: str
            Desired SEC form to return. Form types can and descriptions can 
            be found at https://www.sec.gov/forms
            Default is form 4 (and currently the only one supported)
    :note:
        The SEC is always a day behind so this script will never
        attempt to download today's data
    """
    # create list of urls to download
    if not start:
        start=(_datetime.today()-_timedelta(days=1)).strftime('%Y-%m-%d')
    if not end:
        end=(_datetime.today()-_timedelta(days=1)).strftime('%Y-%m-%d')
        
    urls = ['https://www.sec.gov/Archives/edgar/daily-index/'+
            date.strftime('%Y')+'/QTR'+str((int(date.month)-1)//3 + 1)+
            '/company.'+ date.strftime('%Y%m%d') + '.idx'
            for date in _date_list_generator(start, end)]
    
    # download using threadpool
    results = _download_thread(urls)

    # convert results to pandas dataframe
    dfs = [_idx_to_dataframe(page, url) for page, url in results]
    
    # combine dataframes
    df = _append_dataframes(dfs)
    
    # filter to desired forms
    if form:
        if not isinstance(form, str):
            raise TypeError("form type must be a string")
        else:
            df = df.loc[df['form_type'] == form].reset_index(drop=True)
            
    # create list of urls for each form
    urls = ['https://www.sec.gov/Archives/'+x for x in list(df.file_name)]
        
    # download forms using threadpool
    results = _download_thread(urls)
    
    # qc forms and remove forms hard for the machine to interpret
    dfs = [_form_qc(x[0], x[1]) for x in results]
    dfs = [x for x in dfs if x is not None]
    
    # combine dataframes
    df = _append_dataframes(dfs)
    
    # clean dataframe
    df = _clean_download(df)
    
    # return dataframe
    return df

def _download_thread(urls):
    """requests urls and returns the resulting text and the urls in a list, 
    uses 9 threads due to the SEC's 10 requests per second limit
    :Parameters:
        url: str
            url for request
    """
    pool = _ThreadPool(9)
    results = list(_tqdm.tqdm(pool.imap(_download_one_threaded, urls),
                             total=len(urls)))
    results = [x for x in zip(results,urls) if x[0] is not None]
    pool.terminate()
    pool.join()
    
    return results

def _download_one_threaded(url):
    """requests url and returns the resulting text
    :Parameters:
        url: str
            url for request
    """
    while True:
        try:
            result = _requests.get(url, stream=True)
            formtext = result.text
        except:
            return None
            break
        if 'Request Rate Threshold Exceeded' in formtext:
            print('sleeping: {}'.format(url))
            _sleep(630)
        else:
            break
    if 'AccessDenied' in formtext:
        return None
    else: 
        return formtext

def _date_list_generator(start, end, weekends=False, holidays=False):
    """returns a list of dates between to days, including weekends and 
    holidays are optional
    :Parameters:
        start: str
            Start date string (YYYY-MM-DD) or datetime.
        end: str
            End date string (YYYY-MM-DD) or datetime.
        weekends: boolean
            True - weekends included in list
            False - weekends NOT included in list
            Default is False
        holidays: boolean
            True - holidays included in list
            False - holidays NOT included in list
            Default is False
    """
    
    daterange = _pd.date_range(start, end) if weekends else _pd.bdate_range(start, end)
        
    dfdate = _pd.DataFrame(daterange, columns={'Date'})

    if not weekends:
        holidays = _calendar().holidays(start=daterange.min(), 
                                       end=daterange.max())
        dfdate['Holiday'] = dfdate['Date'].isin(holidays)
        dfdate = dfdate[dfdate['Holiday']==False]
    
    return _pd.to_datetime(dfdate['Date'].unique()).tolist()

def _idx_to_dataframe(idx, idx_url):
    """converts the SEC's idx files into a pandas dataframe
    :Parameters:
        idx: str
            idx file as a string
        idx_url: str
            URL of the idx file for tracking purposes
    """
    return _pd.DataFrame([list(filter(None,[x.strip() for x in y]))+[idx_url] 
                         for y in [x.split('   ') for x in 
                                   idx.splitlines()[11:]]],
                        columns=['company_name', 
                                 'form_type', 
                                 'cik', 
                                 'date_filed',
                                 'file_name',
                                 'idx_name'])

def _append_dataframes(dfs):
    """concatonates dataframes
    :Parameters:
        dfs: list
            List of dataframes
    """
    return dfs[0].append(dfs[1:], ignore_index=True, sort=False)

def _form_qc(formtext, url):
    """returns a dataframe of the relevant data from each form. Returns None 
    if the form is missing info or has too much info, really anything that 
    is hard for the machine to interpret.  There is big room for improvement 
    here to make sure we are collecting as much data as possible
    :Parameters:
        form: str
            The string version of the form file downloaded from the SEC
        url: str
            URL of the form to be recorded into the dataframe
    """    
    datadict = {'URL': [], 'Date':[], 'Issuer CIK': [], 'Issuer Name': [], 
                 'Issuer Ticker': [], 'Owner CIK': [], 'Owner Name': [], 
                 'Director': [], 'Officer': [], 'Ten % Owner': [], 
                 'Officer Title': [], 'Transaction Date': [], 
                 'Transaction Code': [], 'Number of Shares': [], 
                 'Price per Share': [], 'Shares Owned Following Trans': [],
                 'Ownership Nature': []}
    searchlist = ['<periodOfReport>', '<issuerCik>', '<issuerName>', 
                  '<issuerTradingSymbol>', '<rptOwnerCik>', '<rptOwnerName>', 
                  '<transactionDate>', '<transactionShares>', 
                  '<transactionPricePerShare>', 
                  '<sharesOwnedFollowingTransaction>', 
                  '<directOrIndirectOwnership>', '<officerTitle>', 
                  '<isTenPercentOwner>']
    refstrings1 = ['<isDirector>1</isDirector>', '<isOfficer>1</isOfficer>', 
                   '<isDirector>True</isDirector>', 
                   '<isOfficer>True</isOfficer>',
                   '<isDirector>true</isDirector>', 
                   '<isOfficer>true</isOfficer>']
    refstrings2 = ['<transactionCode>S</transactionCode>', 
                   '<transactionCode>P</transactionCode>']

    
    ### debug code
#     error=None
    
#     if any(s in formtext for s in refstrings1):
#         a=1
#     else:
#         error=1
#     if any(p in formtext for p in refstrings2):
#         a=1
#     else:
#         error=2
#     if formtext.count('<reportingOwner>') == 1:
#         a=1
#     else:
#         error=3
#     if formtext.count('<nonDerivativeTransaction>') == 1:
#         a=1
#     else:
#         error=4
#     if all(s in formtext for s in searchlist):
#         a=1
#     else:
#         error=5
        
#     if error:
#         return error
#     else:
#         return 'Success'
    ###
    
    if (any(s in formtext for s in refstrings1) and 
        any(p in formtext for p in refstrings2) and 
        formtext.count('<reportingOwner>') == 1 and
        formtext.count('<nonDerivativeTransaction>') == 1 and
        all(s in formtext for s in searchlist)):

        #loop through lines of file to pull relevant data
        formtext = list(filter(None, formtext.splitlines()))
        nextline = False
        label = None
        datadict['URL'].append(url)

        for line in formtext:

            if nextline:
                datadict[label].append(_re.sub('<[^>]+>', '', line).strip())
                nextline = False
                label = 'ERROR'
                continue

            if '<periodOfReport>' in line and len(datadict['URL']) != len(datadict['Date']):
                datadict['Date'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<issuerCik>' in line and len(datadict['URL']) != len(datadict['Issuer CIK']):
                datadict['Issuer CIK'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<issuerName>' in line and len(datadict['URL']) != len(datadict['Issuer Name']):
                datadict['Issuer Name'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<issuerTradingSymbol>' in line and len(datadict['URL']) != len(datadict['Issuer Ticker']):
                datadict['Issuer Ticker'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<rptOwnerCik>' in line and len(datadict['URL']) != len(datadict['Owner CIK']):
                datadict['Owner CIK'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<rptOwnerName>' in line and len(datadict['URL']) != len(datadict['Owner Name']):
                datadict['Owner Name'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<isDirector>' in line and len(datadict['URL']) != len(datadict['Director']):
                datadict['Director'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<isOfficer>' in line and len(datadict['URL']) != len(datadict['Officer']):
                datadict['Officer'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<isTenPercentOwner>' in line and len(datadict['URL']) != len(datadict['Ten % Owner']):
                datadict['Ten % Owner'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<officerTitle>' in line and len(datadict['URL']) != len(datadict['Officer Title']):
                datadict['Officer Title'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<transactionDate>' in line and len(datadict['URL']) != len(datadict['Transaction Date']):
                nextline = True
                label = 'Transaction Date'
                continue
            if '<transactionCode>' in line and len(datadict['URL']) != len(datadict['Transaction Code']):
                datadict['Transaction Code'].append(_re.sub('<[^>]+>', '', line).strip())
                continue
            if '<transactionShares>' in line and len(datadict['URL']) != len(datadict['Number of Shares']):
                nextline = True
                label = 'Number of Shares'
                continue
            if '<transactionPricePerShare>' in line and len(datadict['URL']) != len(datadict['Price per Share']):
                nextline = True
                label = 'Price per Share'
                continue
            if ('<sharesOwnedFollowingTransaction>' in line and 
                len(datadict['URL']) != len(datadict['Shares Owned Following Trans'])):
                nextline = True
                label = 'Shares Owned Following Trans'
                continue
            if '<directOrIndirectOwnership>' in line and len(datadict['URL']) != len(datadict['Ownership Nature']):
                nextline = True
                label = 'Ownership Nature'
                continue
    else:
        return None
        
    return _pd.DataFrame(datadict)

def _clean_download(df):
    """returns a cleaned SEC download dataframe. Columns cleaned: Director, 
    Officer, Officer Title (comming)
    :Parameters:
        df: pandas dataframe
            An SEC downloaded dataframe
    """
    df['Director'].replace({'0': 'false', '1': 'true'}, inplace=True)
    df['Officer'].replace({'0': 'false', '1': 'true'}, inplace=True)
    return df