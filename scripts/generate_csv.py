import os
import os.path
import json
import multiprocessing
import time
import re
import argparse

import numpy as np

import gab

nodeListFile = 'nodes'
postsListFile = 'posts'

hashTagRE = re.compile(r'(?=^|\s)#[1-9A-Za-z]+')
mentionsRE = re.compile(r'(?=^|\s)@[1-9A-Za-z]+')
urlRE = re.compile(r'(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[a-zA-Z]{2,}(([/:])\S*)?')

newLineRe = re.compile(r'(?<=\w)\s*\n')
whiteSpaceRE = re.compile(r"[^\w#@.!'?’]+")
punctuationSpacingRE = re.compile(r'(?<=\w)([.!?])')

def regexCleaner(s):
    s = urlRE.sub('', s)
    #s = newLineRe.sub('. ', s)
    s = whiteSpaceRE.sub(' ', s).strip()
    s = punctuationSpacingRE.sub(lambda x : ' {}'.format(x.group(1)), s)
    return s

def writeOutputs(suffix, infos, postsFile, usersFile):

    posts = []
    usersInfo = []
    for e in infos:
        if e is not None:
            u, p = e
            usersInfo.append(u)
            posts += p

    with open(postsFile + f"_{suffix}.json", 'a') as f:
        for p in posts:
            f.write(json.dumps(p) + '\n')

    with open(usersFile + f"_{suffix}.json", 'a') as f:
        for u in usersInfo:
            f.write(json.dumps(u) + '\n')

def processEntry(entry, tokenizePosts):
    try:
        G = gab.GabAccount(entry)
    except gab.BadFileError:
        return None
    posts = G.posts()
    if tokenizePosts:
        for p in posts:
            p['clean_body'] = regexCleaner(p['body'])
    usersInfo = G.userInfo()

    return usersInfo, posts

def main():
    parser = argparse.ArgumentParser(description='Convert raw user zips to csvs')
    parser.add_argument('inputDir', type=str, help='raw files dir')
    parser.add_argument('outputDir', type=str, help='New files dir')
    parser.add_argument('--tokenizePosts', help='Add tokenized posts', default=False, action='store_true')
    parser.add_argument('--threads', type=int, help='Max threads', default=32)
    parser.add_argument('--splitFiles', type=int, help='Number of splits, should be less than or equal to threads', default=32)
    args = parser.parse_args()

    outputDir = args.outputDir
    targetsDir = args.inputDir
    num_cores = args.threads
    num_splitFiles = args.splitFiles
    tokenizePosts = args.tokenizePosts

    os.makedirs(outputDir, exist_ok=True)
    postsFile = os.path.join(outputDir, postsListFile)
    usersFile = os.path.join(outputDir, nodeListFile)

    print(f"Starting run {targetsDir} to {outputDir}")
    tStart = time.time()
    with multiprocessing.Pool(processes=num_cores) as pool:
        targets = []
        tCycle = time.time()
        for i, e in enumerate(os.scandir(targetsDir)):
            if not e.name.endswith('.json.gz'):
                continue

            targets.append((e.path, tokenizePosts))
            if i % 100000 == 0 and i > 1:
                print(f"{i}\tProcessing", end = '\t', flush = True)
                infos = pool.starmap(processEntry, targets)
                print("writing", end = '')
                splitInfos = np.array_split(infos, num_splitFiles)
                splitInfos = [(c, list(a), postsFile, usersFile) for c, a in enumerate(splitInfos)]
                pool.starmap(writeOutputs, splitInfos)
                targets = []
                print(", done in {:.2f} minutes".format((time.time() - tCycle) / 60))
                tCycle = time.time()

        print(f"Processing last batch", end = '\t')
        infos = pool.starmap(processEntry, targets, chunksize = 32)
        print("writing")
        splitInfos = np.array_split(infos, num_splitFiles)
        splitInfos = [(c, list(a), postsFile, usersFile) for c, a in enumerate(splitInfos)]
        pool.starmap(writeOutputs, splitInfos)

    print("Done in {:.2f} hours".format((time.time()- tStart) / 60 / 60))

if __name__ == '__main__':
    main()
