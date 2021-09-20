#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  new_testovoe.py
#  
import pandas as pd
from google.cloud import bigquery
import os
import numpy as np
import math
import statistics
import scipy.interpolate as interpolate
import matplotlib.pyplot as plt
from tabulate import tabulate
import time
#from plotly import __version__
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
##from plotly import graph_objs as go
#from fbprophet import Prophet

plt.style.use('seaborn-whitegrid')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "json_key_cloud.json"
client = bigquery.Client()

##################################
query_fulldata = """
	select CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
	totals.totalTransactionRevenue/1e6 AS total_revenue,
	totals.visits as visits,
	totals.hits as hits,
	totals.pageviews as pageviews,
	totals.timeOnSite as timeOnSite,
	totals.newVisits as newvisits,
	trafficSource.source as source,
	device.browser as browser,
	device.operatingSystem as operatingSystem,
	device.deviceCategory as deviceCategory,
	channelGrouping,
	EXTRACT(WEEK FROM PARSE_DATE("%Y%m%d", date)) as date,
	case when totals.totalTransactionRevenue is not NULL then 1 else NULL end as revenue_true,
	case when totals.totalTransactionRevenue is NULL then 1 else NULL end as revenue_false
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
      AND geoNetwork.country = 'United States'
"""

###################################
query_fulldata_analytics = """
with data as (
	select CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
	totals.totalTransactionRevenue/1e6 AS total_revenue,
	totals.visits as visits,
	totals.hits as hits,
	totals.pageviews as pageviews,
	totals.timeOnSite as timeOnSite,
	totals.newVisits as newvisits,
	trafficSource.source as source,
	device.browser as browser,
	device.operatingSystem as operatingSystem,
	device.deviceCategory as deviceCategory,
	channelGrouping,
	case when totals.totalTransactionRevenue is not NULL then 1 else NULL end as revenue_true,
	case when totals.totalTransactionRevenue is NULL then 1 else NULL end as revenue_false
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
      AND geoNetwork.country = 'United States'
)

select * from 
(
select distinct channelGrouping,operatingSystem,
	count(unique_session_id) over w as count_parted,
	count(unique_session_id) over w / count(unique_session_id) over () as count_parted_to_all,
	PERCENTILE_CONT(total_revenue, 0.5) OVER w as median_revenue,
	sum(total_revenue) OVER w as sum_revenue,
	count(total_revenue) over w / count(unique_session_id) over w as revenue_to_parted_all,
	stddev_samp(total_revenue) over w / avg(total_revenue) over w as koef_var_revenue,
	PERCENTILE_CONT(visits, 0.5) OVER w as median_visits,
	stddev_samp(visits) over w / avg(visits) over w as koef_var_visits,
	PERCENTILE_CONT(hits, 0.5) OVER w as median_hits,
	stddev_samp(hits) over w / avg(hits) over w as koef_var_hits,
	PERCENTILE_CONT(pageviews, 0.5) OVER w as median_pageviews,
	sum(pageviews) OVER w as sum_pageviews,
	stddev_samp(pageviews) over w / avg(pageviews) over w as koef_var_pageviews,
	PERCENTILE_CONT(timeOnSite, 0.5) OVER w as median_timeonsite,
	stddev_samp(timeOnSite) over w / avg(timeOnSite) over w as koef_var_timeOnSite
	
from data
WINDOW w AS (partition by channelGrouping,operatingSystem)
   order by sum_revenue desc
   )
   where count_parted >= 500
"""

################################  заворачиваем выборку в словарь просто перебором
"""
query_job = client.query(query_fulldata)
print(time.ctime())
rows = query_job.result()
print(time.ctime())
rows = [dict(i) for i in query_job.result()]
columns = list(rows[0].keys())
all_data_object = dict((k,[]) for k in columns)
print(time.ctime())
for row in rows:
	for k in row:
		all_data_object[k].append(row[k])
print(time.ctime())
np_data_object = {k: np.array(v) for k, v in all_data_object.items()}
"""

########################### временная динамика
date_query = """
select date, sum(total_revenue) as sum
from (
	select CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
	totals.totalTransactionRevenue/1e6 AS total_revenue,
	TIMESTAMP_TRUNC(TIMESTAMP(PARSE_DATE("%Y%m%d", date)), WEEK, 'UTC') as date
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20160601' AND '20170830'
      AND geoNetwork.country = 'United States'
      ) as datetime
      group by date
      order by date
"""
test_query = """
select TIMESTAMP_TRUNC(TIMESTAMP '2008-12-25 15:30:00', WEEK, 'UTC')
"""


#################################  заворачиваем выборку в словарь датафреймом
query_job = client.query(date_query)
rows = query_job.result().to_dataframe()
all_data_object = {k:v for k,v in zip(rows.columns,[np.array(list(filter(lambda v: v==v, rows[i]))) for i in rows])}


plt.rc('font', size=8)
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(all_data_object['date'], all_data_object['sum'], color='tab:orange', label='Total revenues')
ax.set_xlabel('Time')
ax.set_ylabel('Sum revenues')
ax.set_title('Time series')
ax.grid(True)
ax.legend(loc='upper left')
plt.xticks(all_data_object['date'], rotation='vertical')
plt.margins(0.2)
plt.subplots_adjust(bottom=0.15)
plt.show()

"""
def plotly_df(df, title = ''):
    data = []
    
    for column in df.columns:
        trace = go.Scatter(
            x = df.index,
            y = df[column],
            mode = 'lines',
            name = column
        )
        data.append(trace)
    
    layout = dict(title = title)
    fig = dict(data = data, layout = layout)
    iplot(fig, show_link=False)
    
plotly_df(rows, title = 'Sum revenues')
"""

"""
print(rows.describe())

def get_sterges(row, romanov, sterges):
	#romanov:
	#abs(row - avg) < 3.09*sko
	#sterges:
	#n = 1+3.32*lg N
	#print(list(filter(lambda x: ,row)))
	return {'data_filtered':None,'sterges':None}

def distributions_plt(name_value, row):
	fig, axs = plt.subplots(2)
	fig.suptitle('Original and log dist of '+str(name_value))
	#plt.title('Original dist of '+str(name_value))
	#math.ceil(1+3.32*np.log10(len(row)))
	original_sterges = math.ceil(1+3.32*np.log10(len(row)))
	axs[0].hist(row, density=True, bins=original_sterges, ec="yellow", facecolor='g', alpha=0.75)
	#axs[0].set_xticks(np.arange(50, 140,10))
	row_log = np.log10(row)
	log_sterges = original_sterges
	axs[1].hist(row_log, density=True, bins=log_sterges, ec="yellow", facecolor='r', alpha=0.75)
	#axs[1].set_xticks(np.arange(50, 140,10))
	plt.grid(True)
	plt.show()

#poped = ['newvisits', 'revenue_true', 'revenue_false', 'visits', 'unique_session_id']
columns = list(all_data_object.keys())
not_poped = ['timeOnSite', 'pageviews', 'hits', 'total_revenue']
not_used_columns = [all_data_object.pop(c) for c in columns if c not in not_poped]
print(len(all_data_object))
[distributions_plt(k,v) for k,v in all_data_object.items()]
"""

###############################   принтим групбай для суммирующих табличек и графиков
#query_job = client.query(query_fulldata_analytics)
#rows = query_job.result()
#for row in rows:
#	row_d = dict(row)
#	for i in row_d:
#		print("{:<24} {}".format(i,row_d[i]))
#	print()


