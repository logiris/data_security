import pandas as pd
import requests
from bs4 import BeautifulSoup

import time

url = "http://47.117.41.252:33200/index.php?controller=product&action=detail&id="
user_lst = []

def get_data(id):
    r = requests.get(url + str(id))
    soup = BeautifulSoup(r.text, 'html.parser')
    comment = soup.select("body > main > section.product-reviews > div > div")
    for c in comment:
        s = BeautifulSoup(str(c), 'html.parser')
        user_id = int(s.select_one(".user-id").text.split("：")[1])
        user_name = s.select_one(".reviewer-name").text.split("：")[1]
        user_phone = s.select_one(".reviewer-phone").text.split("：")[1]
        user_comment = s.select_one(".review-content").text.strip()
        user_lst.append([user_id, user_name, user_phone, user_comment])
        time.sleep(0.5)

for i in range(1, 501):
    try:
        get_data(i)
    except Exception as e:
        print(i)
        print(e)

df = pd.DataFrame(user_lst, columns=["user_id", "user_name", "user_phone", "user_comment"]).sort_values(by="user_id")
print(df.head())
df.to_csv("task1.csv", index=False)
