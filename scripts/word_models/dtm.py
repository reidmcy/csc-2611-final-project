import pandas
import numpy as np
import json
import glob
import seaborn
import gab

import re
import collections
import urllib
import datetime
import dateutil
import argparse

import pandas
import matplotlib.pyplot as plt
import numpy as np
import seaborn

import ipywidgets as widgets
from IPython.display import display

import json
import gensim
from gensim.models.ldamulticore import LdaMulticore
import multiprocessing
import os
import sys

from gensim.models import LdaSeqModel
from gensim.models.wrappers import DtmModel

def loadFile(filePath):
    #print(f"Reading: {filePath}")
    targets = []
    with open(filePath) as f:
        for i, l in enumerate(f):
            targets.append(json.loads(l))
            if i % 10000 == 0:
                print(f"{filePath}:{i}", end = '\r')
    return pandas.DataFrame(targets)

def loadngrams(path):
    with open(path) as f:
        dat = {}
        f.readline()
        for l in f:
            *w, c = l.strip().split(',')
            dat[tuple(w)] = int(c)
    return dat

def main():
    parser = argparse.ArgumentParser(description='Convert jsons to ngram counts')
    parser.add_argument('--num_topics', type=int, help='num_topics', default=100)
    parser.add_argument('--simple_lda', help='just LDA', default=False, action='store_true')
    parser.add_argument('--ngrams', type=str, help='ngrams files', default="/u/reidmcy/Gab/2019-03-17_ngrams_2/1-grams.csv")
    parser.add_argument('output', type=str, help='output dir name')
    parser.add_argument('inputs', type=str, nargs = '+', help='posts files')

    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    print("Loading data")
    with multiprocessing.Pool(48) as pool:
        print("Loading posts")
        postsDat = pool.map(loadFile, args.inputs)
    df_posts = pandas.concat(postsDat, sort=False)
    print("Tokenizing")
    df_posts['tokens'] = df_posts['clean_body'].apply(lambda x : x.lower().split())
    print("Filtering")
    unigrams = {k[0]: v for k, v in loadngrams(args.ngrams).items()}
    df_posts['tokens_reduced'] = df_posts['tokens'].apply(lambda x : [w for w in x if w in unigrams])

    print("Binning by date")

    df_posts['date'] = df_posts['created_at'].apply(lambda x : x[:7])
    df_posts = df_posts.sort_values('date')
    print("Removing duplicates")
    df_posts = df_posts.groupby('body').first()
    df_posts['body'] = df_posts.index
    df_posts = df_posts.sort_values('date')
    print("Counting")
    df_counts = df_posts[['date','body']].groupby('date').count()

    print("Creating dict")
    dictionary = gensim.corpora.Dictionary(df_posts['tokens_reduced'])
    dictionary.save(os.path.join(args.output,'dlda_dict.mm'))

    print("Creating corpus")
    with multiprocessing.Pool(28) as pool:
        corpus = pool.map(dictionary.doc2bow, df_posts['tokens_reduced'])

    with open(os.path.join(args.output,'dlda_corpus.json'), 'w') as f:
        json.dump(corpus,f)
    time_slices = list(df_counts['body'])

    with open(os.path.join(args.output,'dlda_time_slices.json'), 'w') as f:
        json.dump(time_slices,f)
    print("Running model")
    if args.simple_lda:
        model = LdaMulticore(
            corpus,
            id2word=dictionary,
            num_topics=args.num_topics,
            workers=16,
        )
    else:
        model = DtmModel(
            '/u/reidmcy/.local/bin/dtm-linux64', corpus=corpus, id2word=dictionary,
            time_slices=time_slices,
            num_topics=args.num_topics,
        )
    print("Done, saving")
    model.save(os.path.join(args.output,'DtmModel.mm'))

if __name__ == '__main__':
    main()
