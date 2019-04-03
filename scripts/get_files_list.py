import requests
import json
import time
import urllib
import os
import os.path
import traceback
import datetime
import gzip

import gab_scraper

usersDir = 'users_info'
outputDir = 'files_lists'

def recursiveStringSearch(d):
    ret = []
    for k, v in d.items():
        if isinstance(v, dict):
            ret += recursiveStringSearch(v)
        elif isinstance(v, list):
            for e in v:
                ret += recursiveStringSearch(e)
        elif isinstance(v, str):
            ret += [v]
    return ret

def main():
    print("Starting")
    os.makedirs(outputDir, exist_ok=True)

    targetfiles = {}
    for i, e in enumerate(os.scandir(usersDir)):
        if e.name.endswith('.json.gz'):
            print(f"{i} {[(k, len(v)) for k, v in  targetfiles.items()]} scanning: {e.name}")
            #print(targetfiles)
            try:
                d = gab_scraper.loadRecord(e.path)
            except json.decoder.JSONDecodeError:
                continue
            potURLs = [u for u in recursiveStringSearch(d) if u.startswith('https://files.gab.ai/') and len(u) < 80]
            for url in potURLs:
                fileType = os.path.basename(os.path.dirname(url))

                try:
                    targetfiles[fileType].add(url)
                except KeyError:
                    targetfiles[fileType] = set()
                    targetfiles[fileType].add(url)
        if i % 10000 == 0 and i > 1:
            for k, v in targetfiles.items():
                with open(os.path.join(outputDir, f"{k}.csv"), 'w') as f:
                    for u in v:
                        f.write(u + '\n')

    print("Done")

if __name__ == '__main__':
    main()
