#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  yam_test.py
#  
#  Copyright 2021 user <user@ASSISTANT>
from google.cloud import bigquery
import os
import numpy as np
import scipy.interpolate as interpolate
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')
from tabulate import tabulate
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "json_key_cloud.json"
client = bigquery.Client()

QUERY4 = ("""
with data as(
select unique_session_id, total_revenue, date,
	count(total_revenue) over (partition by date) as count,
	avg(total_revenue) over (partition by date) as avg,
	PERCENTILE_CONT(total_revenue, 0.5) OVER(partition by date) as median,
	stddev_samp(total_revenue) over(partition by date) as sko,
	min(total_revenue) over (partition by date) as min,
	max(total_revenue) over (partition by date) as max,
from
(
    select *
from
(
	select * ,
stddev_samp(logtar) over(partition by country) as sko,
avg(logtar) over(partition by country) as avg
from
  (
  SELECT
      CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
      totals.totalTransactionRevenue/1e6 AS total_revenue,
      log(totals.totalTransactionRevenue/1e6) as logtar,
      date, geoNetwork.country as country
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
      AND totals.totalTransactionRevenue IS NOT NULL
      AND geoNetwork.country = 'United States'
  ) as source_data
) as before_cut
WHERE abs(logtar - avg) < 3.09*sko
   ) as pre_data
)

select * from
(
select distinct date, count, avg, median, min, max, sko, sko/avg as koef_var from data
) as finalyze
order by date
""")

query_job = client.query(QUERY4)
rows = query_job.result()
time_dynamic = [{'date':i.date,'median':round(float(i.median),2),'koef var':round(float(i.koef_var),4),'count':i.count} for i in rows]
columns = tuple(time_dynamic[0].keys())
print(columns)

print("{:<10} {:<10} {:<10} {:<10}".format(columns[0],columns[1],columns[2],columns[3]))
for i in time_dynamic:
    print("{:<10} {:<10} {:<10} {:<10}".format(i.get(columns[0]), i.get(columns[1]), i.get(columns[2]), i.get(columns[3])))

graphs = [('dynamics of the median of transaction price','median revenue',[i['median'] for i in time_dynamic]),
			('dynamics of the variation of transaction price','variation of revenue',[i['koef var'] for i in time_dynamic]),
			('dynamics of the count','count of revenue',[i['count'] for i in time_dynamic])
			]

dates = np.array([i['date'] for i in time_dynamic])
median = np.array([i['median'] for i in time_dynamic])
for i in graphs:

	fig = plt.figure()
	ax = plt.axes()
	ax.plot(dates, np.array(i[2]))
	plt.title(i[0])
	plt.grid(which='minor', alpha=1)
	plt.grid(which='major', alpha=1)
	plt.legend(loc='best')
	plt.xlabel("time")
	plt.ylabel(i[1])
	plt.show()
	plt.show()
#time.sleep(2)
"""
	x = np.array(dates)
	y = np.array(i[2])
	
	plt.title(i[0])
	plt.grid(which='minor', alpha=1)
	plt.grid(which='major', alpha=1)
	plt.legend(loc='best')
	plt.xlabel("time")
	plt.ylabel(i[1])
	plt.show()
	time.sleep(2)
"""

#for i in time_dynamic:
#	print(i.get(columns[0]),i.get(columns[1]),i.get(columns[2]),i.get(columns[3]))




#for row in rows:
#    print(dict(row))

#df = client.query(QUERY).result().to_dataframe()
#print(df['device'])
#print(df.columns.tolist())

#hn_dataset_ref = client.dataset('google_analytics_sample', project='bigquery-public-data')
#hn_dset = client.get_dataset(hn_dataset_ref)
#hn_full = client.get_table(hn_dset.table('ga_sessions_20170801'))
#[print(i) for i in hn_full.schema]

"""
hn_dataset_ref = client.dataset('google_analytics_sample', project='bigquery-public-data')
print(type(hn_dataset_ref))
hn_dset = client.get_dataset(hn_dataset_ref)
print(type(hn_dset))
#print([x.table_id for x in client.list_tables(hn_dset)])
hn_full = client.get_table(hn_dset.table('ga_sessions_20170801'))
print(type(hn_full))
print([command for command in dir(hn_full) if not command.startswith('_')])
#print(hn_full.schema)
#[print(i) for i in hn_full.schema]
schema_subset = [col for col in hn_full.schema]
results = [x for x in client.list_rows(hn_full, start_index=1, selected_fields=schema_subset, max_results=1)]
#print(results)
#for i in results:
#    print(dict(i))
"""

QUERY = ("""
	select * from
	(
    SELECT totals.hits as hits, count(fullVisitorId) as count
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` 
    group by totals.hits
    ) as dt
    order by hits
		""")
		
QUERY = ("""
    SELECT geoNetwork.country as country, count(fullVisitorId) as count
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` 
    group by geoNetwork.country
    order by count
		""")

QUERY = ("""
with data_all as (
		select * from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
	)
select totals.totalTransactionRevenue/1e6, count(fullVisitorId) as count from data_all
group by totals.totalTransactionRevenue
order by count

		""")
		
QUERY = ("""
  SELECT
      CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
      totals.totalTransactionRevenue/1e6 AS total_revenue
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
      AND totals.totalTransactionRevenue IS NOT NULL
"""
)

QUERY1 = ("""
with data as (

	select unique_session_id, total_revenue, logtar, country,
	count(total_revenue) over (partition by country) as count,
	avg(total_revenue) over (partition by country) as avg,
	PERCENTILE_CONT(total_revenue, 0.5) OVER(partition by country) as median,
	stddev_samp(total_revenue) over(partition by country) as sko
	from
	(
    select *
from
(
	select * ,
stddev_samp(logtar) over(partition by country) as sko,
avg(logtar) over(partition by country) as avg
from
  (
  SELECT
      CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
      totals.totalTransactionRevenue/1e6 AS total_revenue,
      date, geoNetwork.country as country
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20160601' AND '20170731'
      AND totals.totalTransactionRevenue IS NOT NULL
      AND geoNetwork.country = 'United States'
  ) as source_data
) as before_cut
WHERE abs(logtar - avg) < 3.09*sko
   ) as after_cut

)

    SELECT distinct country,
    count, avg, median, sko
    FROM data
    where count>20
    order by count
		""")

QUERY2 = ("""
with data as (
select *,
unnest(array(select generate_series(1::numeric,
							base_characteristics.ceil::numeric,
							1::numeric) as degenerate)) as another_unnest
from
(
select *,
	   over_test_max,
	   over_test_min,
	   over_test,
	   ((over_test_max - over_test_min)/(1+3.5*log(over_test))) as step,
	   ceil((over_test_max-over_test_min)/((over_test_max - over_test_min)/(1+3.5*log(over_test)))) as ceil,
	(
		select sum(num) from (SELECT ROW_NUMBER() OVER() AS num
		FROM UNNEST(
			(SELECT GENERATE_ARRAY(1,
		ceil((over_test_max-over_test_min)/((over_test_max - over_test_min)/(1+3.5*log(over_test))))
			) AS h FROM (SELECT NULL))
			) AS pos) as pos_2
	) as sum_another_teor
from (
	select unique_session_id, total_revenue, logtar, country,
	count(total_revenue) over (partition by country) as count,
	avg(total_revenue) over (partition by country) as avg,
	PERCENTILE_CONT(total_revenue, 0.5) OVER(partition by country) as median,
	stddev_samp(total_revenue) over(partition by country) as sko,
	min(total_revenue) over (partition by country) as over_test_min,
	max(total_revenue) over (partition by country) as over_test_max,
	count(total_revenue) over (partition by country) as over_test
	from
	(
    select *
from
(
	select * ,
stddev_samp(logtar) over(partition by country) as sko,
avg(logtar) over(partition by country) as avg
from
  (
  SELECT
      CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
      totals.totalTransactionRevenue/1e6 AS total_revenue,
	  log(totals.totalTransactionRevenue/1e6) as logtar,
      geoNetwork.country as country
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
      _TABLE_SUFFIX BETWEEN '20170501' AND '20170731'
      AND totals.totalTransactionRevenue IS NOT NULL
      and geoNetwork.country is not NULL
      and geoNetwork.country = 'United States'
  ) as source_data
) as before_cut
WHERE abs(logtar - avg) < 3.09*sko
   ) as after_cut
) as first_analyze
) as second_analyze
)

select * from data limit 10

""")

QUERY3 = ("""
select  unnest(GENERATE_ARRAY(1,7,1)) from (select NULL) as un

--select sum(num) from
--	 (SELECT ROW_NUMBER() OVER() AS num
--FROM UNNEST((SELECT GENERATE_ARRAY(1,ceil(6.4)) AS h FROM (SELECT NULL))) AS pos ORDER BY num) as tyu

--select sum(biba) from UNNEST(GENERATE_ARRAY(1,ceil(6.4))) as biba
--SELECT num FROM UNNEST(GENERATE_ARRAY(10, ceil(30.6))) AS num
""")

#SELECT 50 + ROW_NUMBER() OVER() AS num
#FROM UNNEST((SELECT SPLIT(FORMAT("%600s", ""),'') AS h FROM (SELECT NULL))) AS pos
#ORDER BY num
#SELECT sum(num) FROM UNNEST(GENERATE_ARRAY(10, 30)) AS num;
