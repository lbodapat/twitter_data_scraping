import tweepy

client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAAiETgEAAAAAeIhS4UmAwsb079HFfiVFV9cSet0%3DQYf2jirj7YlhYlY7xryopT0XRgpb9zy8oegLVtswFSHBFffZFl',wait_on_rate_limit=True)
#client = tweepy.Client(access_token= "1433793212263682071-FsDVpYfUpf5Hd1nQYzkMEDOuJPvOO2",access_token_secret= "veLak9EPx6EyxVlM4w0LsEDKgPLejEPXkkivJs2DlM0Ss")
# Replace with your own search query
#query = 'from:suhemparack -is:retweet'

tweets = client.search_recent_tweets(query='conversation_id:1467505399775391746', tweet_fields=['in_reply_to_user_id','author_id','created_at','conversation_id'], max_results=100)

for tweet in tweets.data:
    print(tweet)