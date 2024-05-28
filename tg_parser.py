import os
import re
import json
import time
import pandas as pd


INTERVAL = 3600 * 24 * 7 * 4 # 1 month in seconds
FOLDER1 = 'groups\\'   # folder with group names for each news-topic
FOLDER2 = 'grouplst\\' # folder with news for each telegram-group
GROUPS = {}


def preprocess(post, category='other'):
    '''Preprocesses the post'''
    data = {}
    if post['content']:
        data['description'] = post['content']
        data['category'] = category
    return data


def parse_posts(group_name='test', since_time=int(time.time()), category='other'):
    '''Parses posts from a group and saves them to a file'''
    filename = f'{FOLDER1}{group_name}.txt'
    os.system(f'snscrape --jsonl --since {since_time} telegram-channel {group_name} > {filename}')
    with open(f'{FOLDER1}{group_name}.txt', 'r', encoding='utf-8') as f:
        posts = [preprocess(json.loads(x), category) for x in f.read().split('\n')[:-1]]
        posts = '\n'.join(json.dumps(post, ensure_ascii=False) for post in posts if post)

    with open(f'{FOLDER1}{group_name}.txt', 'w', encoding='utf-8') as f:
        f.write(posts)


def parse_groups(groups, since_time):
    '''Parses all groups and saves them to files'''
    for cat_name in groups:
        for group_name in groups[cat_name]:
            parse_posts(group_name, since_time, cat_name)
            print(f'{group_name}: Parsed!')


def main():
    '''Main function'''
    if not os.path.exists(FOLDER1):
        os.makedirs(FOLDER1)

    for fn in os.listdir(FOLDER2):
        with open(f'{FOLDER2}{fn}', 'r', encoding='utf-8') as f:
            GROUPS[fn[:-4]] = f.read().split('\n')

    curr_time = int(time.time())
    since_time = curr_time - INTERVAL
    parse_groups(GROUPS, since_time)

    res = []
    for cat_name in GROUPS:
        for group_name in GROUPS[cat_name]:
            with open(f'{FOLDER1}{group_name}.txt', 'r', encoding='utf-8') as f:
                res.extend(x for x in f.read().split('\n')[:-1])

    with open('parsed_news_info.csv', 'w', encoding='utf-8') as f:
        f.write('\n'.join(res))

    df = pd.read_json('parsed_news_info.csv', lines = True)
    df = df[(df.description.str.len() > 100) &
            (df.description.str.len() < 1000)] # remove short posts
    df.description = df.description.apply(lambda text:
                                          re.sub(r'([а-яa-z\.:!])([А-ЯA-Z\d])', r'\1. \2', text))
    df.description = df.description.apply(lambda text: re.sub(r'[\s]{2,}', r' ', text))
    df.to_csv('parsed_news_info.csv', index=False)

    print('-- Successfully parsed! --')


if __name__ == "__main__":
    main()
