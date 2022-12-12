#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import tweepy
import sqlite3
import time


# **Task:** Load the values of access tokens and keys from environmental variables to python variables

# In[2]:


#consumer_key = os.environ.get('consumer_key')# CODE HERE
#consumer_secret = os.environ.get('consumer_secret') # CODE HERE
#access_token = os.environ.get('access_token') # CODE HERE
#access_token_secret =os.environ.get('access_secret') # CODE HERE
bearer_token = os.environ.get('bearer_token')


# **Task:** Edit function `main` so it can store tweets anywhere (location specified as parameter). The FILTER and LANGUAGES should be parameters as well. Test it with different values and languages.

# In[27]:


###########################
"""
Sream_tweets
"""
##########################
class TweetStream(tweepy.StreamingClient):
    """
    This class will be activted to listen new tweets (real-times) and
    store into DB created
    """
    start_time = time.time()
    def on_connect(self):
        #let us know that we have successfully connected to the twitter API
        print("Connected!")

    def on_tweet(self, tweet):
        #get real-time tweets
        if tweet.referenced_tweets == None: #not re-tweets
            #connect db
            conn = sqlite3.connect("../data/tweets.db")
            cur = conn.cursor()
            #sql queries
            sql_query = """
                        INSERT INTO tweets (userid,tweet)
                        VALUES (?, ?)
                        """
            sql_vals = (tweet.id,tweet.text)
            cur.execute(sql_query, sql_vals)
            conn.commit()
            print("Twitter id:",tweet.id)
            #counting time to close stream
        if time.time() - self.start_time >= time_limit: #set time_limit outside Class
            print(f'Time limit {time_limit}s reached', time.time() - self.start_time)
            tweepy.StreamingClient.disconnect(self)

    def on_disconnect(self):
        #call after disconnect stream
        print("Disconnected")


#set time_limit in second
time_limit = 22
#create the sqlite3 database to store Data from tweets
conn = sqlite3.connect("../data/tweets.db")
print("DB created!")
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS tweets (id, userid TEXT, tweet TEXT)")
print("Table created")
def main():
    stream = TweetStream(bearer_token)
    #..Params for functions
    # Apply rules
    #return current rules
    #stream.get_rules()

    #add new rules
    stream.add_rules(tweepy.StreamRule(value="Python has:mentions lang:en", tag="Python"))
    stream.add_rules(tweepy.StreamRule(value="Data Science has:mentions lang:en", tag="Data Science"))

    #filter to referenced_tweets to get data in on_tweet
    stream.filter(tweet_fields=["text"])

if __name__ == "__main__":
    main()

# **Task:** Create File `stream_tweets.py` that can be executed from the Terminal by exporting the code from this notebook.

# **Task:** Start storing tweets which contain either happy smiley (`:)`) or sad smiley (`:(`) in their text. We will use this dataset during the NLP section. It's good to let the script running for at least 2-3 hours to collect enough data for future modeling.

# > #### Note
# > the main function runs as an ongoing process and won;t stop until you stop it!
