import os
import gzip
import json
import csv
import os
import multiprocessing
import pandas
import time

import gab

targetsDir = '../users_info'
output = '../processed_data/'

attachmentsFile = os.path.join(output, 'attachments.json')

embedsFile = os.path.join(output, 'embeds.json')

num_cores = 30
def processEntry(entry):
    try:
        G = gab.GabAccount(entry)
    except gab.BadFileError:
        return [], []
    attachments = []
    embeds = []
    for p in G['posts']:
        post = p['post']
        if 'attachment' in post:
            attachments.append({
                'id' : post['id'],
                'attachment' : post['attachment'],
            })
        if 'embed' in post:
            embeds.append({
                'id' : post['id'],
                'embed' : post['embed'],
            })

    return attachments, embeds

def writeOutputs(dats):
    with open(attachmentsFile, 'a') as fa, open(embedsFile, 'a') as fe:
        for e in dats:
            attachments, embeds = e
            for a in attachments:
                json.dump(a, fa)
                fa.write('\n')
            for e in embeds:
                json.dump(e, fe)
                fe.write('\n')

def main():
    tStart = time.time()
    with multiprocessing.Pool(processes=num_cores) as pool:
        targets = []
        tCycle = time.time()
        for i, e in enumerate(os.scandir(targetsDir)):
            if not e.name.endswith('.json.gz'):
                continue
            targets.append(e.path)
            if i % 10000 == 0 and i > 1:
                print(f"{i}\tProcessing", end = '\t')
                infos = pool.map(processEntry, targets)
                print("writing", end = '')
                writeOutputs(infos)
                targets = []
                print(", done in {:.2f} minutes".format((time.time() - tCycle) / 60))
                tCycle = time.time()
        print(f"Processing last batch", end = '\t')
        infos = pool.map(processEntry, targets, chunksize = 32)
        print("writing")
        writeOutputs(infos)

    print("Done in {:.2f} hours".format((time.time()- tStart) / 60 / 60))

if __name__ == '__main__':
    main()
