import requests
import pandas as pd
import uuid
import time
import random
import threading
import os
from newspaper import fulltext
from newspaper import Article

API_KEY = None # Use your own api key news api
stop_flags = {}
categories = ["business", "entertainment", "general", "health", "science", "sports", "technology"]


if not os.path.exists('./stage_data'):
    os.makedirs('./stage_data')
    for c in categories:
        df = pd.DataFrame(columns=['id', 'source', 'topic', 'author', 'title', 'url', 'timestamp', 'content'])
        df = df.astype({'content': 'object'})
        df.to_excel(f'./stage_data/{c}_data.xlsx', index=False)


def category_gather(category, pageSize=10):
    df = pd.read_excel(f'./stage_data/{category}_data.xlsx')
    page_number = 1
    while not stop_flags[category]:
        # request
        url = f'https://newsapi.org/v2//top-headlines?country=us&category={category}&pageSize={pageSize}&page={page_number}&apiKey={API_KEY}'
        response = requests.get(url).json()

        # error handling
        if response['status'] == 'error':
            print(f"Error in {category}")
            print(response)
            stop_flags[category] = True
            continue

        # Adding data
        data = []
        total_results = response['totalResults']
        print(f"Processing: {total_results} results at page number {page_number} for {category}")
        
        for resp in response['articles']:
            news_url = resp['url']
            if news_url == 'https://removed.com':
                continue

            print(f"{category} processing {news_url}")

            try:
                article = Article(news_url)
                article.download()
                article.parse()
            except:
                continue

            entry = {
                'id': uuid.uuid4(),
                'source': resp['source']['id'],
                'topic': category,
                'author': resp['author'],
                'title': resp['title'],
                'url': resp['url'],
                'timestamp': resp['publishedAt'],
                'content': article.text
            }

            data.append(entry)
            #print(article.text[:int(len(article.text) * 0.05)] + "........." +  article.text[int(len(article.text) * 0.95):])


            # sleeping for random number of seconds
            random_seconds = random.randint(60, 460)
            print(f" {category} sleeping for {random_seconds // 60} minute(s) and {random_seconds % 60} seconds")
            time.sleep(random_seconds)

        
        # mock database add
        add_df = pd.DataFrame(data, columns=df.columns)
        df = pd.concat([df, add_df])
        df.to_excel(f'./stage_data/{category}_data.xlsx', index=False)
        print(f"All entries for {category} succesfully added")
        
        # page handling logic       
        page_number += 1

        if total_results % pageSize == 0:
            page_limit = (total_results // pageSize)
        else:
            page_limit = (total_results // pageSize) + 1


        if (page_number - 1) == page_limit or (page_number * pageSize) >= 100:
            page_number = 0
            stop_flags[category] = True
            print(f"Reached API limit stopping thread {category}")



threads = []
for category in categories:
    print(f"Starting {category} thread")
    stop_flags[category] = False
    thread = threading.Thread(target=category_gather, args=(category,))
    threads.append(thread)
    thread.start()
    time.sleep(600)

# Wait for all threads to complete
for thread in threads:
    thread.join()

df_lis = []
for category in categories:
    df_lis.append(pd.read_excel(f'./stage_data/{category}_data.xlsx'))

combined = pd.concat(df_lis)
combined.to_excel("combined.xlsx", index=False)
