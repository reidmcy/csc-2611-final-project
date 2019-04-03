import pandas
import json
from nltk.corpus import stopwords

import os
import multiprocessing
import os.path

import nltk

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
outputDir = '/ada/projects/Gab/processed_data/montly_phrases'


maxPhraseLen = 5
targetTerminals = ['NOUN', 'PROPN', 'PRON']

rowsPerBatch = 10000

def getPhrases(s, targets = ('NOUN', 'PROPN', 'PRON'), maxLen = 5):
    sents = s.split('. ')
    rets = []
    for tokens in (nltk.pos_tag(s.split(), tagset='universal') for s in sents):
        words = [w for w,t in tokens]
        splits = [i for i, (w,t) in enumerate(tokens) if t in targets]
        for i, s in enumerate(splits):
            try:
                for j in range(maxLen):
                    rets.append(words[s:splits[i + j + 1] +1])
            except IndexError:
                pass
    return [' '.join(p) for p in rets]

def processRow(row):
    s = json.loads(row['body']).lower()
    month = row['month']
    newPhrases = getPhrases(s, targets = targetTerminals, maxLen = 5)
    return month, newPhrases


def main():
    print("Loading data")
    df = pandas.read_csv(target)
    df['month'] = pandas.to_datetime(df['created_at'])
    df['month'] = df['month'].apply(lambda x : x.strftime("%Y-%m"))
    l = len(df)
    months = {}
    print("Starting run")

    done = False
    count = 0
    rowsIter = df.iterrows()
    while not done:
        batchRows = []
        for i in range(rowsPerBatch):
            try:
                batchRows.append(next(rowsIter)[1])
                count += 1
            except StopIteration:
                done = True
                break
        with multiprocessing.Pool(16) as pool:
            #import pdb; pdb.set_trace()
            results = pool.map(processRow, batchRows)

        for month, newPhrases in results:
            try:
                phrases = months[month]
            except KeyError:
                months[month] = {}
                phrases = months[month]
            for p in newPhrases:
                try:
                    phrases[p] += 1
                except KeyError:
                    phrases[p] = 1
        if i % 100 == 0:
            print("{:.2f}% {}".format(count / l * 100, len(phrases)), end = '\r')
    print("\nDone reading, found {} months".format(len(months)))
    os.makedirs(outputDir, exist_ok=True)
    for m in months:
        output = os.path.join(outputDir, f"{m}.csv")
        phrases = months[m]
        print(f'Writing {m}')
        with open(output, 'w') as f:
            f.write('count, phrase\n')
            for p, n in sorted(phrases.items(), key = lambda x : x[1], reverse=True):
                if n > 5:
                    f.write("{},{}\n".format(n, p))
    print('Done')

if __name__ == '__main__':
    main()
