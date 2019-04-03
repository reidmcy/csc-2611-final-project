import gensim
import gab
import json

import multiprocessing
import argparse

import os
import os.path

import time
from nltk.corpus import stopwords #For stopwords

sWords = set(stopwords.words('english'))
punctation = set(['.', '?', '!'])

def genRowTokens(row):
    s = row.lower()
    sents = s.split('. ')
    return [s.strip().split() for s in sents]

def loadFile(filePath):
    print(f"Reading: {filePath}")
    targets = []
    with open(filePath) as f:
        for i, l in enumerate(f):
            d = json.loads(l)
            m = '-'.join(d['created_at'].split('-')[:2])
            b = d['clean_body']
            if b is not None and len(b) > 0:
                targets.append((m, b))
            if i % 10000 == 0:
                print(f"{filePath}:{i}", end = '\r')
        print(f"{filePath}:read {i} lines")
    return targets

def main():
    parser = argparse.ArgumentParser(description='Convert jsons to Word2Vec')
    parser.add_argument('--threads', type=int, help='Max threads', default=8)
    parser.add_argument('--debug', help='run fast', default=False, action='store_true')
    #parser.add_argument('--w2vDim', type=int, help='num dims', default=256)
    parser.add_argument('output', type=str, help='output dir name')
    parser.add_argument('inputs', type=str,nargs = '+', help='data files')

    args = parser.parse_args()

    os.makedirs(args.output)

    print("Loading data")
    with multiprocessing.Pool(args.threads) as pool:
        targets = pool.map(loadFile, args.inputs)

    print("Preping for tokenizing")
    months = {}
    for fileTargets in targets:
        for m, t in fileTargets:
            name = "before" if gab.isBefore(m) else "after"
            try:
                months[name].append(t)
            except KeyError:
                months[name] = [t]
        if args.debug:
            break

    if args.debug:
        print("----DEBUG MODE-----")
        for m in list(months.keys()):
            months[m] = months[m][:1000]

    print("Tokenizing, stemming")
    monthSents = {}
    with multiprocessing.Pool(args.threads) as pool:
        for m, vals in months.items():
            print(f"Processing: {m}")
            monthSents[m] = pool.map(genRowTokens, vals)

    print("Flattening")
    monthFullSents = {}
    for m, vals in monthSents.items():
        monthFullSents[m] = []
        for ps in vals:
            monthFullSents[m] += ps

    print("Setting up jobs")
    fullSents = []
    for v in monthFullSents.values():
        fullSents += v

    print("Starting full run")
    for hs in [0, 1]:
        for window in [10]:
            for size in [300]:
                for sample in [0.01]:
                    tStart = time.time()
                    print(f"Starting hs_{hs}-window_{window}-size_{size}-sample_{sample} run")
                    modelFull = gensim.models.Word2Vec(fullSents,
                        hs = hs, #Hierarchical softmax is slower, but better for infrequent words
                        size = size, #Dim
                        window = window, #Might want to increase this
                        min_count = 50,
                        max_vocab_size = None,
                        workers = args.threads, #Almost all the cores
                        sample = sample,
                        )

                    modelFull.save(os.path.join(args.output, f'full-hs_{hs}-window_{window}-size_{size}-sample_{sample}.pd'))
                    print(f"Done in {time.time() - tStart:.0f}s")
                    for m, sents in monthFullSents.items():
                        print(f"Starting {m} hs_{hs}-window_{window}-size_{size}-sample_{sample} run")
                        modelFull = gensim.models.Word2Vec(sents,
                            hs = hs, #Hierarchical softmax is slower, but better for infrequent words
                            size = size, #Dim
                            window = window, #Might want to increase this
                            min_count = 50,
                            max_vocab_size = None,
                            workers = args.threads, #Almost all the cores
                            sample = sample,
                            )
                        modelFull.save(os.path.join(args.output, f'{m}-hs_{hs}-window_{window}-size_{size}-sample_{sample}.pd'))
                        print(f"Done in {time.time() - tStart:.0f}s")
    print("Done")

if __name__ == '__main__':
    main()
