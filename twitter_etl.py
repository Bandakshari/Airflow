import pandas as pd 
import json
import tweepy
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key =  "MPqrcbqUnIrcLp1urMLsDGLd2"
    access_secret = "taxdEzMz64CbKVg7pfEgpTiULJ6evjfowXBEkA4SB1jghvEgw8"
    consumer_key = "1605642460901756929-ijK4pLTwtUJlBl4REhMaWkHwmWZuKc"
    consumer_secret = "0bGnCMwJDEH1nhUJ5BM9sHxqYT0FFqICYKI3VaVK1ISxC"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # Creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@ElonMusk', 
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = False,
                                # Necessary to keep full_text 
                                # otherwise only the first 140 words are extracted
                                tweet_mode = 'extended'
                                )
    print(tweets)

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
            
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://bandhu-airflow-bucket/TweetsElonMusk.csv')
