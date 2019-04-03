import os
import os.path
import json

import gab_scraper

saveFile = 'missing.csv'

outputDir = 'users_info'

def main():
    followers = set()
    collected = set()
    for i, e in enumerate(os.scandir(outputDir)):
        if e.name.endswith('.gz'):
            print(f"{i / (len(followers) + 1) * 100:.2f}%\t{i}\t{len(followers)}\tscanning: {e.name}")
            try:
                d = gab_scraper.loadRecord(e.path)
            except json.decoder.JSONDecodeError:
                continue
            collected.add(d['user_info']['username'])
            followers |= set([f['username'] for f in d['followers']])
            followers |= set([f['username'] for f in d['following']])

    missing = followers - collected
    print(f"Found missing: {len(missing)}")
    with open(saveFile, 'w') as f:
        for u in missing:
            f.write(u + '\n')
    print("Done")

if __name__ == '__main__':
    main()
