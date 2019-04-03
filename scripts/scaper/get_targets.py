import os
import os.path
import json
import multiprocessing
import traceback
import json
import datetime

import gab

targets = 'targets.csv'

outputDir = 'users_info'

failsName = 'fails.csv'
errorsLog = 'errors.log'

def downloadTarget(usernames):
    print("Starting new seesion")
    GS = gab.GabSession(username = '', password = '', login = False)
    successes = []
    fails = []
    newUserNames = []
    for i, m, username in usernames:
        print(f"Downloading: {username} {i} of {m} {i / m * 100:.2f}%")
        try:
            with gab.UserFetcher(username, outputDir) as UF:
                try:
                    newTargets = UF.download(GS, raiseExceptions = True)
                except Exception as e:
                    fails.append([username, traceback.format_exc()])
                if newTargets is None:
                    fails.append([username, "Is private"])
                else:
                    successes.append(username)
                    newUserNames += newTargets
        except Exception as e:
            fails.append([username, traceback.format_exc()])

        with open(errorsLog, 'a') as f:
            for u, es in fails:
                f.write(json.dumps({"time" : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'username' : u, 'exception' : es}))
                f.write('\n')
    return successes, fails, list(set(newUserNames))

def prepTargets(tLst, maxPerBin = 100):
    ret = []
    s = []
    for i, t in enumerate(tLst):
        s.append((i, len(tLst), t))
        if i % maxPerBin == 0:
            ret.append(s)
            s = []
    ret.append(s)
    return [l for l in ret if len(l) > 0]

def main():
    with open(targets) as f:
        currentTargets = [l.strip() for l in f]

    print(currentTargets[:10])
    collected = set()
    failed = set()
    print("Starting")
    os.makedirs(outputDir, exist_ok=True)

    while True:
        workingTargets = prepTargets(currentTargets)
        if len(workingTargets) < 1:
            break

        print(workingTargets[:5])

        with multiprocessing.Pool(32) as pool:
            results = pool.map(downloadTarget, workingTargets)

        newTargets = set()
        for succs, fails, news in results:
            collected |= set(succs)
            failed |= set([u for u, e in fails])
            newTargets |= set(news)

        currentTargets = list(newTargets - collected - failed)

        with open(failsName, 'a') as f:
            for fn in failed:
                f.write(fn + '\n')
    print("Done")

if __name__ == '__main__':
    main()
