import pandas
import json
from nltk.corpus import stopwords

sWords = set(stopwords.words('english') + ['.', '?', '!'])

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
output = '/ada/projects/Gab/processed_data/posts_trigrams.csv'

def main():
    print("Loading data")
    df = pandas.read_csv(target)
    l = len(df)
    words = {}
    print("Starting run")
    for i, row in df.iterrows():
        s = json.loads(row['body']).lower()

        try:
            w1, w2, *postWords = [w for w in s.split(' ') if w not in sWords]
        except ValueError:
            continue
        for w in postWords:
            wT = f"{w1},{w2},{w}"
            try:
                words[wT] += 1
            except KeyError:
                words[wT] = 1
            w1 = w2
            w2 = w
        if i % 100000 == 0:
            print("{:.2f}% {}".format(i / l * 100, len(words)), end = '\r')
    print("\nDone reading, found {} bigrams".format(len(words)))
    with open(output, 'w') as f:
        f.write('word1,word2,word3,count\n')
        for w, n in sorted(words.items(), key = lambda x : x[1], reverse=True):
            if n > 10:
                f.write("{},{}\n".format(w, n))
    print('Done')

if __name__ == '__main__':
    main()
