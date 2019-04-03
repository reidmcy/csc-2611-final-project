import gensim
import pandas
#import swifter

import json
import re
import multiprocessing

import os
import os.path

import networkx as nx


target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'

output = '/ada/projects/Gab/processed_data/repost_networks'

def genNetwork(df, outputName):
    df_reposts = df[df['type'] == 'repost']

    edges = {}

    for i, (_, r) in enumerate(df_reposts.iterrows()):

        if i % 1000 == 0:
            print(f"{i / len(df_reposts) * 100:0.2f}%, {len(edges)} edges", end = '\r')

        source = r['orginal_auth']
        target = r['creator']
        name = (source, target)

        if name not in edges:
            edges[name] = {
                'count' : 0,
                'url' : 0,
                'image' : 0,
                'youtube' : 0,
                'hash_tags' : set(),
                'mentions' : set(),
                'urls' : set(),
            }
        edges[name]['count'] += 1
        if r['attachment_type'] is 'url':
            edges[name]['url'] += 1
        elif r['attachment_type'] is 'image':
            edges[name]['image'] += 1
        elif r['attachment_type'] is 'youtube':
            edges[name]['youtube'] += 1
        edges[name]['hash_tags'] |= set(eval(r['hash_tags']))
        edges[name]['mentions'] |= set(eval(r['mentions']))
        edges[name]['urls'] |= set(eval(r['urls']))

    print(f"Done {len(edges)} edges".ljust(40))

    fname = os.path.join(output, f'{outputName}_edges.csv')
    print(f"Writing to: {fname}")

    header = ['Source', 'Target'] + sorted(list(edges.values())[0].keys())
    with open(fname, 'w') as f:
        f.write(','.join(header) + '\n')
        for (source, target), dat in edges.items():
            f.write(f"{source},{target},")
            datInfo = []
            for k in sorted(dat.keys()):
                v = dat[k]
                if isinstance(v, int):
                    datInfo.append(f"{v}")
                else:
                    if len(v) > 0:
                        datInfo.append('"[{}]"'.format(','.join(v)))
                    else:
                        datInfo.append('')
            f.write(','.join(datInfo))
            f.write('\n')

    print("Doing pagerank")

    G = nx.DiGraph()
    G.add_edges_from(edges.keys())

    for e, dat in edges.items():
        G.edges[e]['weight'] = dat['count']

    p = nx.pagerank(G)

    pname = os.path.join(output, f'{outputName}_pagerank.json')

    print(f"Writing to: {pname}")
    with open(pname, 'w') as f:
        json.dump(p, f)
    return edges, G

def main():
    print("Loading data")
    df = pandas.read_csv(target)
    df['month'] = pandas.to_datetime(df['created_at'])
    df['month'] = df['month'].apply(lambda x : x.strftime("%Y-%m"))

    os.makedirs(output, exist_ok=True)

    print("Doing full run")

    genNetwork(df, 'full')

    for m in sorted(df['month'].unique()):
        print(f"Doing: {m}")

        genNetwork(df[df['month'].eq(m)], m)
    print("done")

if __name__ == '__main__':
    main()
