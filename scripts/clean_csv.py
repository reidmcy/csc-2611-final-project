import gab

import json
import pandas
import swifter


target = '../processed_data/posts.csv'
output = '../processed_data/posts_cleaned.csv'


def main():
    df = pandas.read_csv(target)
    df['body'] = df['body'].swifter.apply(gab.cleanMessage)
    df.to_csv(output)

if __name__ == '__main__':
    main()
