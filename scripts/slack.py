import os
import time
import datetime
import sys
import json
import gzip

import requests
import humanize
import pytz
import numpy as np

testing = True if len(sys.argv) > 1 else False

outputDir = 'users_info'

tz = pytz.timezone('Canada/Eastern')

with open('webhook.txt') as f:
    webhook_url = f.read().strip()

def humanTime(t):
    if t > 5:
        return humanize.naturaldelta(t)
    else:
        return "{:.3f} seconds".format(t)

def postMessage(message):
    response = requests.post(webhook_url, data=json.dumps({'text': message}), headers={'Content-Type': 'application/json'})
    return response

def genReport():
    tGenStart = time.time()
    sizes = []
    mtimes = []
    locks = []
    followers = set()
    collected = set()
    for i, e in enumerate(os.scandir(outputDir)):
        if e.name.endswith('.gz'):
            sizes.append(e.stat().st_size)
            mtimes.append(e.stat().st_mtime)
            try:
                with gzip.open(e.path, 'rt') as f:
                        d = json.load(f)
                collected.add(d['user_info']['username'])
                followers |= set([f['username'] for f in d['followers']])
            except json.decoder.JSONDecodeError:
                pass
            except MemoryError:
                pass
            if i % 1000 == 0:
                print(f"Scanning {i}: {e.name}".ljust(79), end = '\r')
        try:
            if e.name.endswith('.lock'):
                locks.append((e.name, e.stat().st_mtime))
        except FileNotFoundError:
            pass
    timeGenerated = datetime.datetime.now(tz)

    lastUp = max(mtimes)

    if testing:
        m = "`TESTING`"
    else:
        m = ''
    m += f'\n*Gab Report*\n_Generated: {timeGenerated.strftime("%Y-%m-%d %H:%M:%S")}_'

    m += """```
|     Name    |       Value      |
|------------:|:-----------------|
| Count       |{} |
| Size        |{} |
| Obs Members |{} |
| Percent Com |{} |
| Remaining   |{} |
| Mean Size   |{} |
| Median Size |{} |
| Last Update |{} |
| Since Last  |{} |
```""".format(str(len(sizes)).rjust(17),
            humanize.naturalsize(sum(sizes), format='%.0f').rjust(17),
            str(len(followers | collected)).rjust(17),
            "{:.2f}%".format(len(collected) / len(followers | collected) * 100).rjust(17),
            "{:.0f}".format(len(followers | collected) - len(collected)).rjust(17),
            humanize.naturalsize(np.mean(sizes), format='%.0f').rjust(17),
            humanize.naturalsize(np.median(sizes), format='%.0f').rjust(17),
            datetime.datetime.fromtimestamp(lastUp).strftime("%Y-%m-%d %H:%M").rjust(17),
            humanTime(time.time() - lastUp).rjust(17),
            )

    m += """\n```
|     Lock    |     Created    |
|------------:|:---------------|
"""
    for n, created in locks:
        m += "|{} |{} |\n".format(n.rjust(12)[:12], humanTime(time.time() - created).rjust(15)[:15])
    m += "```"
    m += "\n_Report generated in: {}_".format(humanTime(time.time() - tGenStart))
    if testing:
        m += "\n`TESTING`"
    postMessage(m)

def main():
    lastRun = time.time()
    try:
        while True:
            s = genReport()
            if testing:
                break
            currentRun = time.time()
            nextRun = lastRun + 60 * 60 * 6 # 6 hours
            while time.time() < nextRun:
                print("Sleeping for {:.0f}m {:.0f}s".format((nextRun - time.time()) / 60, (nextRun - time.time()) % 60).ljust(70), end = '\r')
                time.sleep(5)
            lastRun = currentRun
    except KeyboardInterrupt:
        pass
    except Exception as e:
        postMessage(f"*Reporter Crashed*\n{e}")
        raise


if __name__ == '__main__':
    main()
