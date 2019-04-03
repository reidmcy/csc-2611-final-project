import gensim
import pandas
import swifter

import json
import re
import multiprocessing


import nltk
from nltk.corpus import stopwords #For stopwords

sWords = set(stopwords.words('english'))
punctation = set(['.', '?', '!'])

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
output = '/ada/projects/Gab/processed_data/posts_word2vec.pd'

puncRE = re.compile('[.?!]')

w2vDim = 256

def tokenizer(s):
    s = json.loads(s).lower()

    sents = []
    sent = []
    for w in s.split(' '):
        if w in sWords:
            pass
        elif w in punctation:
            sents.append(sent)
            sent = []
        else:
            sent.append(w)
    return sents

def main():
    print("Reading")
    df = pandas.read_csv(target, nrows = 5000)

    print("Generating vocab")
    vocab = df['body'].swifter.apply(tokenizer)

    print("Getting counts")
    counts = {}
    for v in vocab:
        for w in v:
            try:
                counts[w] += 1
            except KeyError:
                counts[w] += 1
    print(list(vocab)[:60])


if __name__ == '__main__':
    main()
