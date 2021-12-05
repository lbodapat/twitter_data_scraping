'''
@author: Souvik Das
Institute: University at Buffalo
'''

import tweepy


class Twitter:

    tweets = []
    '''
    lang_en,lang_hi,lang_es=0,0,0
    country_us,country_india,country_mex=0,0,0
    covid=0
    '''
    def __init__(self):
        self.auth = tweepy.OAuthHandler("SvX1COY5LDLBSXgT0zFZ8Q5T3", "25urRNXrihwPw9Z6kd1FnWj7KQasqG0u0GrHhznoH9bqVfCyU3")
        self.auth.set_access_token("1433793212263682071-FsDVpYfUpf5Hd1nQYzkMEDOuJPvOO2", "veLak9EPx6EyxVlM4w0LsEDKgPLejEPXkkivJs2DlM0Ss")
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def _meet_basic_tweet_requirements(self,tweet):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        
        if tweet['tweet_lang'] == 'en':
            lang_en += 1
        elif tweet['tweet_lang'] == 'hi':
            lang_hi += 1
        else:
            lang_es += 1
            
        if tweet['country'] == 'USA':
            country_us += 1
        elif tweet['country'] == 'India':
            country_india += 1
        else:
            country_mex += 1
            '''
    '''
	if (tweet.full_text.lower()).startswith("rt @"):
            return 0
	elif tweet.retweeted is True:
	    return 0
        elif lang:
	    if lang == tweet.lang:
		return 1
	    else:
		return 0
        else:
	    return 1
	    '''
        #raise NotImplementedError

    def get_tweets_by_poi_screen_name(self,screenName,count):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        #tweets = self.api.user_timeline(screen_name=screenName,count=c, tweet_mode='extended')
        tweets = tweepy.Cursor(self.api.user_timeline, screen_name=screenName, tweet_mode='extended', include_rts=False).items(count)
        
        tweet_list = [[tweet._json['user']['screen_name'], tweet._json['user']['id'], tweet._json['user']['verified'], tweet._json['id'], tweet._json['full_text'], tweet._json['lang'], tweet._json['entities'], tweet._json['created_at'],tweet._json['favorite_count'],tweet._json['user']['followers_count'],tweet._json['retweet_count'],tweet._json['user']['profile_image_url_https']] for tweet in tweets]
        
        return tweet_list
              

    def get_tweets_by_lang_and_keyword(self,keyword,language,count):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''
        tweets = tweepy.Cursor(self.api.search, q=keyword + '-filter:retweets', lang=language, tweet_mode='extended', since="2020-09-21", until="2021-09-21").items(count)
        
        tweet_list = [[tweet._json['user']['verified'], tweet._json['id'], tweet._json['full_text'], tweet._json['lang'], tweet._json['entities'], tweet._json['created_at']] for tweet in tweets]
        
        return tweet_list
        #raise NotImplementedError

    def get_replies(self, username, tweetId):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        replies=[]
        for tweet in tweepy.Cursor(self.api.search,q='to:'+username, timeout=999999, tweet_mode='extended', since_id=tweetId).items(3000):
            reply_count = 0
            if (reply_count>10):
                break
            if hasattr(tweet, 'in_reply_to_status_id_str'):
                if (tweet.in_reply_to_status_id_str==tweetId):
                    #print(tweet)
                    replies.append(tweet)
                    reply_count += 1
        
        reply_list = [[reply._json['user']['verified'], reply._json['id'], "India",reply._json['in_reply_to_status_id'], reply._json['in_reply_to_user_id'], reply._json['full_text'], reply._json['lang'], reply._json['entities'], reply._json['created_at']] for reply in replies]
        #print(reply_list)
        return reply_list
        #raise NotImplementedError

