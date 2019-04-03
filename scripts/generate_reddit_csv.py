import os
import os.path
import json
import multiprocessing
import time
import re
import bz2
import zstandard as zstd

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

def writeOutputs(suffix, posts, postsFile):
    with open(postsFile + f"_{suffix}.json", 'a') as f:
        for p in posts:
            f.write(json.dumps(p) + '\n')

def processEntry(entry):
    try:
        dat = json.loads(entry)
        body = dat['body']
    except:
        return None
    b =  regexCleaner(body)
    if len(b) > 0:
        return b
    else:
        return None

def zLineReader(filename):
    #Mostly from:
    #https://www.reddit.com/r/pushshift/comments/ajmcc0/information_and_code_examples_on_how_to_use_the/
    with open(filename, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                string_data = chunk.decode('utf-8')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line
                    yield line
                previous_line = lines[-1]

def main():
    parser = argparse.ArgumentParser(description='Convert raw user zips to csvs')
    parser.add_argument('inputfile', type=str, help='raw file')
    parser.add_argument('outputDir', type=str, help='New files dir')
    parser.add_argument('--threads', type=int, help='Max threads', default=32)
    parser.add_argument('--splitFiles', type=int, help='Number of splits, should be less than or equal to threads', default=32)
    args = parser.parse_args()

    outputDir = args.outputDir
    inputfile = args.inputfile
    num_cores = args.threads
    num_splitFiles = args.splitFiles

    os.makedirs(outputDir, exist_ok=True)
    postsFile = os.path.join(outputDir, postsListFile)

    print(f"Starting run {inputfile} to {outputDir}")
    tStart = time.time()
    with multiprocessing.Pool(processes=num_cores) as pool:
        targets = []
        tCycle = time.time()
        for i, l in enumerate(zLineReader(inputfile)):
        #with bz2.open(inputfile, 'rt') as f:
            #for i, l in enumerate(f):
            targets.append(l)
            if i % 100000 == 0 and i > 1:
                print(f"{i}\tProcessing", end = '\t', flush = True)
                infos = pool.map(processEntry, targets)
                print("writing", end = '')
                splitInfos = np.array_split(infos, num_splitFiles)
                splitInfos = [(c, b, postsFile) for c, b in enumerate(splitInfos)]
                pool.starmap(writeOutputs, splitInfos)
                targets = []
                print(", done in {:.2f} minutes".format((time.time() - tCycle) / 60))
                tCycle = time.time()

        print(f"Processing last batch", end = '\t')
        infos = pool.map(processEntry, targets, chunksize = 32)
        print("writing")
        splitInfos = np.array_split(infos, num_splitFiles)
        splitInfos = [(c, b, postsFile) for c, b in enumerate(splitInfos)]
        pool.starmap(writeOutputs, splitInfos)

    print("Done in {:.2f} hours".format((time.time()- tStart) / 60 / 60))

if __name__ == '__main__':
    main()
