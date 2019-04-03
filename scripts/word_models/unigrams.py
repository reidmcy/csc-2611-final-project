import pandas
import json

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
output = '/ada/projects/Gab/processed_data/posts_unigrams.csv'

def main():
    df = pandas.read_csv(target)
    l = len(df)
    words = {}
    for i, row in df.iterrows():
        s = json.loads(row['body'])
        for w in s.split(' '):
            try:
                words[w] += 1
            except KeyError:
                words[w] = 1
        if i % 100000 == 0:
            print("{:.2f}% {}".format(i / l * 100, len(words)), end = '\r')
    print("Done reading, found {} words".format(len(words)))
    with open(output, 'w') as f:
        f.write('word,count\n')
        for w, n in sorted(words.items(), key = lambda x : x[1], reverse=True):
            f.write("{},{}\n".format(w, n))
    print('Done')

if __name__ == '__main__':
    main()
