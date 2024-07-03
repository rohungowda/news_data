import psycopg2
from psycopg2 import OperationalError, Error
import pandas as pd
import uuid
import time
import random

conn = None
try:


    cursor = conn.cursor()

except (Exception, Error, OperationalError) as error:
    print("Error while connecting to PostgreSQL", error)

df = pd.read_excel('combined_modeling.xlsx')
df = df.sample(frac=1.0).reset_index(drop=True)

index = 0
chunk_size = 25

while index < len(df):
    batch_data = df.loc[index:index+chunk_size,['id', 'topic', 'timestamp','article']].values
    log_data = [[str(uuid.uuid4()),batch[1],batch[0]] for batch in batch_data]


    insert_main_query = "INSERT INTO main_news_table (id, topic, timestamp, article) VALUES (%s, %s, %s, %s)"
    insert_log_query = "INSERT INTO log_news_table (log_id, topic, id) VALUES (%s, %s, %s)"

    try:
        cursor.executemany(insert_main_query,batch_data)
        cursor.executemany(insert_log_query,log_data)
        conn.commit()
        print("Batch Succesfully Sent")
    except psycopg2.Error as e:
        conn.rollback()  # Rollback changes if an error occurs
        print("Batch Failed:", e)
        exit(0)

    index += (chunk_size + 1)
    exit(0)
    # create kafka consumer for 
    random_seconds = random.randint(60, 460)
    print(f"Sleeping for {random_seconds // 60} minute(s) and {random_seconds % 60} seconds")
    time.sleep(random_seconds)

cursor.close()
conn.close()


'''
            ResultSet resultSet = statement.executeQuery("SELECT * from news_data");

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String topicName = resultSet.getString("topic_name");
                String author = resultSet.getString("author");
                Date timestamp = resultSet.getDate("timestamp");
                String title = resultSet.getString("title");
                String article = resultSet.getString("article");

                // Print the results
                System.out.println("ID: " + id);
                System.out.println("Topic Name: " + topicName);
                System.out.println("Author: " + author);
                System.out.println("Timestamp: " + timestamp);
                System.out.println("Title: " + title);
                System.out.println("Article: " + article);
                System.out.println("--------------------------");
            }
'''