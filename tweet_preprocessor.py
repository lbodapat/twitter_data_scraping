'''
@author: Souvik Das
Institute: University at Buffalo
'''

import demoji
import re
import datetime
import preprocessor
from twitter import Twitter
import string

# demoji.download_codes()

class TWPreprocessor:

    @classmethod
    def preprocess(
        cls,
        tweet,
        country,
        poiFlag,tweet_text
        ):
        '''
        Do tweet pre-processing before indexing, make sure all the field data types are in the format as asked in the project doc.
        :param tweet:
        :return: dict
        '''
        
        
        if poiFlag:
            #Getting hashtags, mentions and tweet_urls
            hashtags=_get_entities(tweet[6],'hashtags')
            mentions=_get_entities(tweet[6],'mentions')
            tweet_urls=_get_entities(tweet[6],'urls')
            
            text_cleaned_data=_text_cleaner(tweet[4],tweet[5],hashtags,mentions,tweet_urls)
            tweet_dict = {
                'poi_name': tweet[0],
                'poi_id': tweet[1],
                'verified': tweet[2],
                'country': country,
                'id': tweet[3],
                'tweet_text': tweet[4],
                'text_' + tweet[5]: text_cleaned_data[0],
                'tweet_lang': tweet[5],
                'hashtags': _get_entities(tweet[6], 'hashtags'),
                'mentions': _get_entities(tweet[6], 'mentions'),
                'tweet_urls': _get_entities(tweet[6], 'urls'),
                'tweet_emoticons': text_cleaned_data[1],
                'tweet_date': str(_get_tweet_date(tweet[7])),
                'favorite_count': tweet[8],
                'followers_count': tweet[9],
                'retweet_count': tweet[10],
                'profile_image_url_https': tweet[11],
                'media_url': tweet[6]['media']['media_url_https'] if hasattr(tweet[6], 'media') else ""
                }
        else:
            #Getting hashtags, mentions and tweet_urls
            hashtags=_get_entities(tweet[7],'hashtags')
            mentions=_get_entities(tweet[7],'mentions')
            tweet_urls=_get_entities(tweet[7],'urls')
            
            text_cleaned_data=_text_cleaner(tweet[5],tweet[6],hashtags,mentions,tweet_urls)
            tweet_dict = {
                'verified': tweet[0],
                'country': country,
                'id': tweet[1],
                'replied_to_tweet_id': tweet[3],
                'replied_to_user_id': tweet[4],
                'reply_text': tweet[5],
                'tweet_text': tweet_text,
                'text_' + tweet[6]: text_cleaned_data[0],
                'tweet_lang': tweet[6],
                'hashtags': _get_entities(tweet[7], 'hashtags'),
                'mentions': _get_entities(tweet[7], 'mentions'),
                'tweet_urls': _get_entities(tweet[7], 'urls'),
                'tweet_emoticons': text_cleaned_data[1],
                'tweet_date': str(_get_tweet_date(tweet[8]))
                }

        # raise NotImplementedError

        # twitter._meet_basic_tweet_requirements(tweet_dict)

        return tweet_dict


def _get_entities(tweet, type=None):
    result = []
    if type == 'hashtags':

        # hashtags = tweet['entities']['hashtags']

        hashtags = tweet['hashtags']
        for hashtag in hashtags:
            result.append(hashtag['text'])
    elif type == 'mentions':
        mentions = tweet['user_mentions']

        for mention in mentions:
            result.append(mention['screen_name'])
    elif type == 'urls':
        urls = tweet['urls']

        for url in urls:
            result.append(url['url'])

    return result


def _text_cleaner(text,lang,hashtags,mentions,tweet_urls):
    emoticons_happy = list([
        ':-)',
        ':)',
        ';)',
        ':o)',
        ':]',
        ':3',
        ':c)',
        ':>',
        '=]',
        '8)',
        '=)',
        ':}',
        ':^)',
        ':-D',
        ':D',
        '8-D',
        '8D',
        'x-D',
        'xD',
        'X-D',
        'XD',
        '=-D',
        '=D',
        '=-3',
        '=3',
        ':-))',
        ":'-)",
        ":')",
        ':*',
        ':^*',
        '>:P',
        ':-P',
        ':P',
        'X-P',
        'x-p',
        'xp',
        'XP',
        ':-p',
        ':p',
        '=p',
        ':-b',
        ':b',
        '>:)',
        '>;)',
        '>:-)',
        '<3',
        ])
    emoticons_sad = list([
        ':L',
        ':-/',
        '>:/',
        ':S',
        '>:[',
        ':@',
        ':-(',
        ':[',
        ':-||',
        '=L',
        ':<',
        ':-[',
        ':-<',
        '=\\',
        '=/',
        '>:(',
        ':(',
        '>.<',
        ":'-(",
        ":'(",
        ':\\',
        ':-c',
        ':c',
        ':{',
        '>:\\',
        ';(',
        ])
    all_emoticons = emoticons_happy + emoticons_sad

    emojis = list(demoji.findall(text).keys())
    clean_text = demoji.replace(text, '')

    for emo in all_emoticons:
        if emo in clean_text:
            clean_text = clean_text.replace(emo, '')
            emojis.append(emo)
    
    #Data Cleaning for EN
    if(lang=='en'):
        clean_text = preprocessor.clean(text)
     #Data clean for ES and HI
    if(lang=='hi' or lang=='es'):
        clean_text="".join([i for i in text if i not in string.punctuation])
        clean_text="".join([i for i in text if i not in emojis])
        for ht in hashtags:
            temp=clean_text.replace(ht,'')
            clean_text=temp
        for men in mentions:
             temp=clean_text.replace(men,'')
             clean_text=temp
        for url in tweet_urls:
              temp=clean_text.replace(url,'')
              clean_text=temp
        for p in string.punctuation:
              temp=clean_text.replace(p,'')
              clean_text=temp
    
    #clean_text = preprocessor.clean(text)

    # preprocessor.set_options(preprocessor.OPT.EMOJI, preprocessor.OPT.SMILEY)
    # emojis= preprocessor.parse(text)

    return (clean_text, emojis)


def _get_tweet_date(tweet_date):
    return _hour_rounder(datetime.datetime.strptime(tweet_date,
                         '%a %b %d %H:%M:%S +0000 %Y'))


def _hour_rounder(t):

    # Rounds to nearest hour by adding a timedelta hour if minute >= 30

    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) \
        + datetime.timedelta(hours=t.minute // 30)

