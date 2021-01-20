import sys

import nltk
import pandas as pd

from time import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()
i = 0

df = pd.read_csv('result.csv', header=0, error_bad_lines=False, encoding = "ISO-8859-1", engine="python")
shape = df.shape

def get_sentiment(review):
    global i, start_time
    i += 1
    if (i%10000 == 0):
        percentage = i/shape[0]
        elapsed = time() - start_time
        prediction = elapsed / percentage
        left = prediction - elapsed

        print(f'{i}/{shape[0]} - {percentage*100:.2f}% - ETA {left:.1f}s')
    try:
        return analyzer.polarity_scores(review)['compound']
    except:
        return 0

start_time = time()
df['sentiment'] = df.review.apply(get_sentiment)

df.drop(columns=['review'])

df.to_csv(f'result_sent.csv', columns=['listing_id', 'sentiment'])
