import pymysql
from sqlalchemy import create_engine
import matplotlib.dates as mdates
import matplotlib.cbook as cbook

import pandas_datareader as pdr
import datetime 
import pandas as pd
import matplotlib.patches as mpatches
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU
from sklearn.preprocessing import MinMaxScaler

date_start = datetime.datetime(2017, 2, 16)
date_end = datetime.datetime(2018, 4, 16)
load_op = False

def get_ticker_data_single(ticker,begin,finish):
  try:
    data = pdr.get_data_yahoo(ticker,start = begin,end=finish)
    return data
  except :
      return None

def get_ticker_data_n(ticker,begin,finish,n):
    counter = 0
    ret = None
    for i in range(1,n):
        data = get_ticker_data_single(ticker,begin,finish)
        counter = counter + 1
        if data is not None:
            ret = data
            break
    return counter,ret

slSQL=['SELECT moment,short_position',
'FROM stock.openpos2', 
'WHERE contract_type="%s" AND iz_fiz=%d AND isin LIKE "%s"',
'AND moment between "%s" AND "%s"',
'ORDER by moment DESC;']

sSQL="\n".join(slSQL)


def draw_ticker_data(pticker,pdate_start,pdate_end):
    cnt,df=get_ticker_data_n(pticker,pdate_start,pdate_end,10)
	
    date_range = pd.date_range(pdate_start,pdate_end)
	
    if load_op:
      cnx = create_engine('mysql+pymysql://max:root@localhost/stock', echo=False)
      df_op = pd.read_sql(sSQL % ('F',0,pticker,df.index.min(),df.index.max()), cnx)
      df_op['short_position_scaled'] = MinMaxScaler().fit_transform((df_op['short_position']).values.reshape(-1,1))
      df_op.index = df_op.moment

    # Calculate the RSI based on EWMA
    delta = df['Close'].diff()

    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0

    roll_up1 = up.ewm(com=7,min_periods=0,adjust = True,ignore_na = False).mean()
    roll_down1 = down.abs().ewm(com=7,min_periods=0,adjust = True,ignore_na = False).mean()

    df['RSI'] = (100.0 - (100.0 / (1.0 + roll_up1 / roll_down1))) / 100

    step=(df['High']-df['Low']).mean()/2
    minp=df['Low'].min()
    maxp=df['High'].max()
    price_range=np.arange(minp,maxp,step)
    price_range_bar_counts=np.ndarray(price_range.shape)
    price_range_bar_counts[:]=0

    df['Mid']=((df['High']-df['Low'])/2)+df['Low']

    df['BB'] = df['Mid'].rolling(window=20,min_periods=0).mean()
    df['BB_width'] = df['Mid'].rolling(window=20,min_periods=0).std()*2

    for level_idx,level in enumerate(price_range):
      bigger = df['High'] > level
      smaller = df['Low'] < level
      price_range_bar_counts[level_idx]=(df[bigger & smaller])['High'].count()

    df=df.reindex(date_range)
    df.fillna(method="ffill",inplace=True)

    if load_op:
      df_op=df_op.reindex(date_range)
      df_op.fillna(method="ffill",inplace=True)

    years = mdates.YearLocator()   # every year
    months = mdates.MonthLocator()  # every month
    days = mdates.DayLocator(interval=7)  # every 
    mondays = mdates.WeekdayLocator(byweekday=MO)
    yearsFmt = mdates.DateFormatter('%Y')
    monthFmt = mdates.DateFormatter('%m-%Y')

    fig = plt.figure(figsize=(15,10))
    ax1 = plt.subplot2grid((16,1),(0,0),rowspan=10,colspan=1)   
    ax2 = plt.subplot2grid((16,1),(11,0),rowspan=2,colspan=1,sharex = ax1)
    ax3 = plt.subplot2grid((16,1),(14,0),rowspan=2,colspan=1,sharex = ax1)
    

    ax1.plot(df.index,df['Mid'],color = "k",linewidth=1)
    black_patch = mpatches.Patch(color = 'k', label=pticker+' Mid price')
    ax1.legend(handles=[black_patch])

    for ax in fig.get_axes():
      ax.grid(b=True, which='minor')
      ax.grid(b=False, which='major')
      for tick in ax.get_xticklabels():
        tick.set_rotation(30)

    ax1.plot(df.index,df['BB'],color = "r",linewidth=.5)
    ax1.plot(df.index,df['BB']+df['BB_width'],color = "r",linewidth=.5)
    ax1.plot(df.index,df['BB']-df['BB_width'],color = "r",linewidth=.5)

    for level_idx,level in enumerate(price_range):
      color = (plt.cm.get_cmap('terrain_r'))( (price_range_bar_counts[level_idx]*2)/df['Mid'].count() )
      ax1.axhline(y=level,color = color)

    #ax2.plot(df.index,df['Volume'],color = "b",linewidth=.5)
    ax2.bar(df.index,df['Volume'],width=1,color = "b",linewidth=.5)
	
    volume_patch = mpatches.Patch(color = 'b', label=pticker+' Volume')
    ax2.legend(handles=[volume_patch])


    RSI_patch = mpatches.Patch(color = 'r', label=pticker+' RSI')       
    ax3.legend(handles=[RSI_patch])
	
    if load_op:
      ax3.plot(df_op.index,df_op.short_position_scaled,color = "g",linewidth=.5)
    ax3.plot(df.index,df.RSI,color = "r",linewidth=.5)
	
    ax3.axhline(y=0.4,color = "c",linewidth=.5)

    ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_formatter(monthFmt)
    ax1.xaxis.set_minor_locator(mondays)

    datemin = datetime.date(df.index.min().year, df.index.min().month, 1)
    datemax = datetime.date(df.index.max().year, df.index.max().month+1, 1)
    ax1.set_xlim(datemin, datemax)

    ax1.format_xdata = mdates.DateFormatter('%Y-%m-%d')

    plt.show()

	
tickers=['AFLT.ME', 'ALRS.ME', 'FEES.ME', 'GAZP.ME', 'HYDR.ME', 'MOEX.ME', 'MTSS.ME', 'NLMK.ME', 'ROSN.ME', 'RTKM.ME', 'SBER.ME', 'SBERP.ME', 'SNGS.ME', 'SNGSP.ME', 'TATN.ME', 'VTBR.ME','RSTI.ME']
for ticker in tickers:
    draw_ticker_data(ticker,date_start,date_end)
