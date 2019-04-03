import pandas
import json
from nltk.corpus import stopwords

import os
import os.path

sWords = set(stopwords.words('english') + ['.', '?', '!'])

target = '/ada/projects/Gab/processed_data/posts_cleaned.csv'
outputDir = '/ada/projects/Gab/processed_data/montly_trigrams'

def main():
    print("Loading data")
    df = pandas.read_csv(target)
    df['month'] = pandas.to_datetime(df['created_at'])
    df['month'] = df['month'].apply(lambda x : x.strftime("%Y-%m"))
    l = len(df)
    months = {}
    print("Starting run")
    for i, row in df.iterrows():
        s = json.loads(row['body']).lower()
        month = row['month']
        try:
            words = months[month]
        except KeyError:
            months[month] = {}
            words = months[month]
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
    print("\nDone reading, found {} bigrams".format(len(months)))
    os.makedirs(outputDir, exist_ok=True)
    for m in months:
        output = os.path.join(outputDir, f"{m}.csv")
        words = months[m]
        print(f'Writing {m}')
        with open(output, 'w') as f:
            f.write('word1,word2,word3,count\n')
            for w, n in sorted(words.items(), key = lambda x : x[1], reverse=True):
                if n > 10:
                    f.write("{},{}\n".format(w, n))
    print('Done')

if __name__ == '__main__':
    main()
