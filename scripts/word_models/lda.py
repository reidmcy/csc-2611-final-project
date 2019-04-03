import gensim
import pandas
#import swifter

import json
import re
import multiprocessing

import os
import os.path

import nltk
import nltk.stem.porter
from nltk.corpus import stopwords #For stopwords

from gensim import corpora, models

import gensim.models.ldamulticore


sWords = set(stopwords.words('english'))
punctation = set(['.', '?', '!'])

sWords |= punctation

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
output = '/ada/projects/Gab/processed_data/posts_lda_multi_2'


stemmer = nltk.stem.porter.PorterStemmer()

def genRowTokens(row):
    s = json.loads(row['body']).lower()

    words = s.split()

    words_filtered = [stemmer.stem(w) for w in words if w not in sWords]

    month = row['month']
    return month, words_filtered


def main():
    print("Loading data")
    df = pandas.read_csv(target)
    df['month'] = pandas.to_datetime(df['created_at'])
    df['month'] = df['month'].apply(lambda x : x.strftime("%Y-%m"))
    l = len(df)
    monthWords = {}

    print("Tokenizing, loading")

    inputDat = []
    for i, r in df.iterrows():
        inputDat.append({"month" : r['month'], 'body' : r['body']})


    print("Tokenizing, stemming")
    with multiprocessing.Pool(24) as pool:
        stemmed = pool.map(genRowTokens, inputDat)

    print("Tokenizing, binning")
    for m, words in stemmed:
        try:
            monthWords[m].append(words)
        except KeyError:
            monthWords[m] = [words]

    print("Cleaning up")
    del inputDat
    del stemmed

    os.makedirs(output, exist_ok=True)

    print("Starting full run")

    fullWords = []
    for m in monthWords.values():
        fullWords += m

    print("Creating dictionary")
    dictionaryFull = corpora.Dictionary(fullWords)

    print("Creating corpus")
    corpusFull = [dictionaryFull.doc2bow(text) for text in fullWords]

    print("Creating lda model")
    ldaFull = gensim.models.ldamulticore.LdaMulticore(
        corpusFull,
        workers = 16,
        num_topics = 128,
        id2word = dictionaryFull,
        passes = 1,
        )

    print("Saving")
    ldaFull.save(os.path.join(output, 'full.lda'))

    del dictionaryFull
    del corpusFull
    del fullWords

    print("Done")

    return

    for m, words in monthWords.items():
        print(f"Starting {m} run")
        print("Creating dictionary")
        dictionary = corpora.Dictionary(words)

        print("Creating corpus")
        corpus = [dictionary.doc2bow(text) for text in words]

        print("Creating lda model")
        lda = gensim.models.ldamulticore.LdaMulticore(
            corpus,
            workers = 4,
            num_topics = 128,
            id2word = dictionary,
            passes = 1,
            )

        print("Saving")
        lda.save(os.path.join(output, f"{m}.lda"))


if __name__ == '__main__':
    main()
