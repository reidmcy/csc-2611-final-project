import pandas
import json
import argparse
import os
import multiprocessing

from nltk.corpus import stopwords

sWords = set(stopwords.words('english') + ['.', '?', '!'])

def loadFile(filePath, drop_reposts = False):
    print(f"Reading: {filePath}")
    targets = []
    with open(filePath) as f:
        for i, l in enumerate(f):
            d = json.loads(l)
            m = '-'.join(d['created_at'].split('-')[:2])
            b = d['clean_body']
            if drop_reposts and d['creator'] != d['orginal_auth']:
                continue
            if b is not None and len(b) > 0:
                targets.append((m, b))
            if i % 10000 == 0:
                print(f"{filePath}:{i}", end = '\r')
        print(f"{filePath}:read {i} lines")
    return targets

def loadRedditFile(filePath):
    print(f"Reading: {filePath}")
    targets = []
    with open(filePath) as f:
        for i, l in enumerate(f):
            b = json.loads(l)
            if b is not None and len(b) > 0:
                targets.append(('2017-11', b))
            if i % 10000 == 0:
                print(f"{filePath}:{i}", end = '\r')
        print(f"{filePath}:read {i} lines")
    return targets

def genTokens(row):
    s = row.lower()
    return [w for w in s.strip().split() if w not in sWords]

def genNgrams(n, texts):
    words = {}
    for j, post in enumerate(texts):
        for i in range(len(post) - n + 1):
            wT = ",".join(post[i:i+n])
            try:
                words[wT] += 1
            except KeyError:
                words[wT] = 1
        if j % 100000 == 0:
            print("{}-gram: {:.2f}% {}".format(n, j / len(texts) * 100, len(words)), end = '\r')
    return words

def processNgrams(n, texts, outputName):
    if os.path.isfile(outputName):
        print(f"Skipping: {outputName}")
        return
    if isinstance(texts, str):
        texts = json.loads(texts)
    print(f"Starting: {n} {outputName}")
    words = genNgrams(n, texts)
    print(f"Done creating: {n}")
    print(f"Writing {n}")
    with open(outputName, 'w') as f:
        for i in range(n):
            f.write(f"word{i},")
        f.write('count\n')
        for w, n in sorted(words.items(), key = lambda x : x[1], reverse=True):
            if n > 10:
                f.write("{},{}\n".format(w, n))
    print(f"Done {n} {outputName}")


def main():
    parser = argparse.ArgumentParser(description='Convert jsons to ngram counts')
    parser.add_argument('--threads', type=int, help='Max threads', default=32)
    parser.add_argument('--debug', help='run fast', default=False, action='store_true')
    parser.add_argument('--no-reposts', help='drop reposts', default=False, action='store_true')
    parser.add_argument('--reddit', help='run fast', default=False, action='store_true')
    parser.add_argument('output', type=str, help='output dir name')
    parser.add_argument('inputs', type=str,nargs = '+', help='data files')

    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("Loading data")
    with multiprocessing.Pool(args.threads) as pool:
        if args.reddit:
            targets = pool.map(loadRedditFile, args.inputs)
        else:
            targets = pool.starmap(loadFile, [(a, args.no_reposts) for a in args.inputs])

    print("Preping for tokenizing")
    months = {}
    for fileTargets in targets:
        for m, t in fileTargets:
            try:
                months[m].append(t)
            except KeyError:
                months[m] = [t]
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
            monthSents[m] = pool.map(genTokens, vals)

    print("Setting up jobs")
    fullSents = []
    for v in monthSents.values():
        fullSents += v
    fullSents = json.dumps(fullSents)

    jobs = []
    for i in range(3):
        jobs.append((i+1, fullSents, os.path.join(args.output, f"{i+1}-grams.csv")))
        for m, v in monthSents.items():
            jobs.append((i+1, json.dumps(v), os.path.join(args.output, f"{i+1}-grams_{m}.csv")))

    print("Starting jobs")
    for job in jobs:
        processNgrams(*job)
    #with multiprocessing.Pool(args.threads) as pool:
    #    rets = pool.starmap(processNgrams, jobs)
    print("Done")

if __name__ == '__main__':
    main()
