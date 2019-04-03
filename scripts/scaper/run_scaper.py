import os
import os.path
import json
import multiprocessing
import traceback
import json
import datetime

import gab

previousDir = '2019-03-17-users_info'
outputDir = '2019-03-28-users_info'

failsName = 'fails.csv'
errorsLog = 'errors.log'

num_processes = 86

def loadTargets(dat):
    i, path = dat
    if i % 1000 == 0:
        print(f"{i} loading {path}".ljust(70).replace('\n', ''), end = '\r')
    try:
        A = gab.GabAccount(path)
    except gab.BadFileError:
        return []
    return list(set(A.followers + A.following)) + [A.name]

def downloadTarget(usernames):
    print("Starting new session")
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

    loadPaths = []
    for e in os.scandir(previousDir):
        if e.name.endswith('json.gz'):
            loadPaths.append(e.path)

    print("Loading targets")
    with multiprocessing.Pool(num_processes) as pool:
        newTargets = pool.map(loadTargets, enumerate(loadPaths))

    print("\nDepuplicating targets")
    currentTargets = set()
    for tL in newTargets:
        currentTargets |= set(tL)
    print(f"{len(currentTargets)} targets found")

    collected = set()
    failed = set()
    print("Starting")
    os.makedirs(outputDir, exist_ok=True)

    while True:
        workingTargets = prepTargets(currentTargets)
        if len(workingTargets) < 1:
            break

        with multiprocessing.Pool(num_processes) as pool:
            try:
                results = pool.map(downloadTarget, workingTargets)
            except KeyboardInterrupt:
                print("\nExiting")
                return

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
