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
outputDir = 'files'
locksDir = 'image_locks'

fileList = 'files_lists'

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

class ImagesFetcher(object):
    def __init__(self, path):
        self.path = path
        self.username = os.path.basename(path)
        self.lockFile = f"{locksDir}/{self.username}.lock"
        self.outputFile = f"{locksDir}/{self.username}.done"
        self.lock = None
        self.lockFileHandle = None

    def __enter__(self):
        if os.path.isfile(self.outputFile):
            self.lock = False
            #raise LockError(f"{self.username} already downloaded/or being download")
        else:
            try:
                self.lockFileHandle = open(self.lockFile, 'x')
            except FileExistsError:
                self.lock = False
            else:
                self.lock = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.lock:
            self.lockFileHandle.close()
            os.remove(self.lockFile)

    def download(self, session):
        if not self.lock:
            print(f"{self.username} already downloaded")
            return []
        else:
            print(f"Downloading: {self.username}")

        d = gab_scraper.loadRecord(self.path)

        potURLs = recursiveStringSearch(d)
        dls = 0
        for u in potURLs:
            if u.startswith('https://files.gab.ai/'):
                try:
                    ret = session.downloadImage(u, outputDir)
                except RuntimeError:
                    pass
                except requests.exceptions.Timeout:
                    print("Timeout occured, sleeping and reseting session")
                    time.sleep(60)
                    session.setupSession()
                except gab_scraper.PrivateAccount:
                    continue
                else:
                    if ret:
                        print(f"\tDownloaded: {u}")
                        dls += 1
        try:
            with open(self.outputFile, 'x') as f:
                f.write(str(dls))
        except FileExistsError:
            pass


def main():
    print("Starting")
    os.makedirs(outputDir, exist_ok=True)
    os.makedirs(locksDir, exist_ok=True)
    GS = gab_scraper.GabSession(login = False)

    for i, e in enumerate(os.scandir(usersDir)):
        if e.name.endswith('.json.gz'):
            with ImagesFetcher(e.path) as UF:
                newTargets = UF.download(GS)

    print("Done")

if __name__ == '__main__':
    main()
