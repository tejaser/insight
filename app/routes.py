from flask import render_template
from app import app
import pandas as pd
import calendar


all_segment = pd.read_csv("app/data/all_segment_month7.csv")
orders = pd.read_csv("app/data/orders_month7.csv")
visits = pd.read_csv("app/data/top_customer_visits_month7.csv")
####
#
####
c_year = orders.YEAR[0]
c_month = calendar.month_name[orders.MONTH[0]]
total_user_count = all_segment.shape[0]
total_order_count = all_segment.frequency.sum()
total_order_value = all_segment.monetory_value.sum()
top_users = all_segment[all_segment['RFMScore'] == 111].sort_values('monetory_value', ascending=False)
top_user_order_value = top_users.monetory_value.sum()
total_value = all_segment.monetory_value.sum()
top_user_count = top_users.shape[0]
top_user_order_value = top_users.monetory_value.sum()

top_user_order_count = top_users.frequency.sum()
top_user_ratio = (top_user_count / total_user_count) * 100
top_user_order_value_ratio = (top_user_order_value / total_order_value) * 100
top_user_order_count_ratio = (top_user_order_count / total_order_count) * 100
(desktop_user, mobile_user) = visits.groupby('DEVICE')['USER_ID'].count()
country_list = visits.groupby('COUNTRY_NAME')['USER_ID'].count().sort_values(ascending=False)
(cart, checkout)=visits.groupby('CONVERSION')['USER_ID'].count()
(returning, first_time)=visits.groupby('USER_TYPE')['USER_ID'].count()
mobile_ratio=(mobile_user/(mobile_user+desktop_user)*100)
cart_ratio=((cart/(cart+checkout))*100)
first_time_ratio=((first_time/(first_time+returning))*100)



@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Tejas'}
    return render_template('index.html',
                           title='bebe', user=user, total_user=total_user_count, c_year=c_year,
                           c_month=c_month, user_ratio=top_user_ratio, value_ratio=top_user_order_value_ratio,
                           order_ratio=top_user_order_count_ratio, mobile_ratio=mobile_ratio,
                           first_time_ratio=first_time_ratio,top_country=country_list, cart_ratio=cart_ratio)
