import nltk
import pandas
import os
import json
import multiprocessing

targetPosts = 'processed_data/posts.csv'
posts_output = 'processed_data/posts_tokenized.json'
targetNodes = 'processed_data/nodes.csv'
nodes_output = 'processed_data/nodes_tokenized.json'

def processLine(l):
    return  json.dumps(nltk.word_tokenize(json.loads(l)))

def main():
    with multiprocessing.Pool(processes=8) as pool:
        df_posts = pandas.read_csv(targetPosts, index_col=0)
        print("Loaded")
        with open(posts_output, 'w') as f:

            #rowsIter = df_posts.iterrows()
            targets = []
            for i, r in df_posts.iterrows():
                #print(r)
                targets.append(r['body'])

                if i % 100000 == 0 and i > 0:
                    results = pool.map(processLine, targets)

                    targets = []
                    f.write('\n'.join(results))
                    f.write('\n')

                    print("posts: {}\t{:.2f}%\t{}".format(str(i).rjust(12), i/len(df_posts) * 100, str(r['body'])[:20]))
            results = pool.map(processLine, targets)

            targets = []
            f.write('\n'.join(results))
            f.write('\n')

    df_nodes = pandas.read_csv(targetNodes, index_col=0)
    with open(nodes_output, 'w') as f:
        for i, r in df_nodes.iterrows():
            json.dump(nltk.word_tokenize(json.loads(r['bio'])), f)
            f.write('\n')
            if i % 10000 == 0:
                print("nodes: {}\t{:.2f}%\t{}".format(str(i).rjust(12), i/len(df_nodes) * 100, str(r['bio'])[:20]))

    print("Done")

if __name__ == '__main__':
    main()
