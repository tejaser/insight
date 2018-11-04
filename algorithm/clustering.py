import pandas as pd
import sqlalchemy
import datetime as dt
engine = sqlalchemy.create_engine('mysql+pymysql://lxrinsight:lxrinsight@192.168.0.84:3306/lxrinsight_new', pool_size=10, max_overflow=20)
new_query = """

SELECT BUSINESS_ASSOCIATE_ID,
                   IP2INT(nt.IP_ADDRESS)                        AS IP_FROM,
                   nt.USER_ID,
                   nt.SESSIONID,
                    SERVER_TIMESTAMP,
                   CURRENT_URL,
                   HOUR(SERVER_TIMESTAMP)                       as HOUR,
                   DAYNAME(SERVER_TIMESTAMP)                    as DAY,
                   MONTH(SERVER_TIMESTAMP)                      as MONTH,
                   YEAR(SERVER_TIMESTAMP)                       as YEAR,
                   USER_TYPE,
                   CASE WHEN ORDER_ID IS null THEN 0 ELSE 1 END as IS_CONVERTED,
                   CASE
                     WHEN DEVICE_TYPE = 'Personal computer' THEN 'DESKTOP'
                     ELSE 'MOBILE' END                          as DEVICE,
                   CASE
                     WHEN LANDING_TYPE IN (1, 4, 5) THEN 'PPC'
                     WHEN LANDING_TYPE = 2 THEN 'ORGANIC'
                     WHEN LANDING_TYPE = 3 THEN 'Direct'
                     ELSE 'OTHERS' END                          as CLICK_SOURCE,
                   CASE
                     WHEN SE_ID IN (1) THEN 'Yahoo'
                     WHEN SE_ID IN (2) THEN 'Google'
                     WHEN SE_ID IN (3) THEN 'Bing'
                     WHEN SE_ID IN (5) THEN 'Facebook'
                     WHEN SE_ID IN (6) THEN 'Duckduckgo'
                     WHEN SE_ID IN (7) THEN 'Pinterest'
                     WHEN SE_ID IN (8) THEN 'Instagram' END     as PPC_TYPE,
                   x.*
            FROM NE_TRACKING_INFO nt
                   INNER JOIN (SELECT PRODUCT_NAME,
                                      ORDER_ITEM_ID,
                                      ne.ORDER_ID,
                                      NE_TRACKING_ID,
                                      QUANTITY,
                                      ner.PRICE as PRICE,
                                      TAX,
                                      SHIPPING
                               FROM NE_ORDER ne,
                                    NE_ORDER_ITEMS ner
                               WHERE ne.ORDER_ID = ner.ORDER_ID
                                 AND BUS_ASS_ID = 1462
                                 AND TIME_STAMP
                                         BETWEEN '2018-06-31 23:59:59.000000'
                                         AND '2018-07-31 23:59:59.000000'
                               GROUP BY ne.ORDER_ID, ner.PRODUCT_NAME, ner.ORDER_ITEM_ID) x
            WHERE x.NE_TRACKING_ID = nt.NE_TRACKING_ID
              AND (nt.CONVERSION = 1)
              AND BUSINESS_ASSOCIATE_ID = 1462
              AND SERVER_TIMESTAMP BETWEEN '2018-06-31 23:59:59.000000'
                      AND '2018-07-31 23:59:59.000000'
            GROUP BY SESSIONID, PRODUCT_NAME, ORDER_ITEM_ID
"""
df = pd.read_sql_query(new_query, engine, parse_dates={'SERVER_TIMESTAMP'})
df['TOTAL'] = df['PRICE'] + df['TAX'] + df['SHIPPING']
df = df[(df['TOTAL'] > 0)]
NOW = dt.datetime(year=2018, month=8, day=1)
rfmTable = df.groupby('USER_ID').agg({'SERVER_TIMESTAMP': lambda x: (NOW - x.max()).days,
                                      'ORDER_ID': lambda x: len(x),
                                      'TOTAL': lambda x: x.sum()})
rfmTable['SERVER_TIMESTAMP'] = rfmTable['SERVER_TIMESTAMP'].astype(int)
rfmTable.rename(columns={'SERVER_TIMESTAMP': 'recency',
                         'ORDER_ID': 'frequency',
                         'TOTAL': 'monetory_value'}, inplace=True)
quantiles = rfmTable.quantile(q=[0.25, 0.5, 0.75])
quantiles = quantiles.to_dict()
segmented_rfm = rfmTable


def RScore(x, p, d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.5]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4


def FMScore(x, p, d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.5]:
        return 3
    elif x <= d[p][0.75]:
        return 2
    else:
        return 1


segmented_rfm['r_quartile'] = segmented_rfm['recency'].apply(RScore, args=('recency', quantiles,))
segmented_rfm['f_quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency', quantiles,))
segmented_rfm['m_quartile'] = segmented_rfm['monetory_value'].apply(FMScore, args=('monetory_value', quantiles,))

segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)

final_df = segmented_rfm[segmented_rfm['RFMScore'] == '111'].sort_values('monetory_value', ascending=False).head(100)
