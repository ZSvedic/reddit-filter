import orjson

def load_jsonl(file_path):
    with open(file_path, 'rb') as file:
        for line in file:
            yield orjson.loads(line)

def save_jsonl(file_path, data_list):
    with open(file_path, 'w', encoding='utf-8') as file:
        for entry in data_list:
            file.write(orjson.dumps(entry).decode() + '\n')

def valid_submission(sub_json, score, title, selftext, url, min_sub_score, max_text_len):
    ''' Only create a submission if it: 
        - has a score above min, and
        - total len is shorter than max_text_len, and
        - title is not "[deleted by user]", and
        - selftext is not "[removed]" or "[deleted]", and
        - links to itself (reddit link) and not an image, video or external link, and
        - doesn't start with a link, and
        - doesn't have a thumbnail. '''

    total_len = len(title) + len(selftext)

    if score < min_sub_score or \
        total_len <= 1 or \
        total_len > max_text_len or \
        title.startswith('[deleted by user]') or \
        selftext.startswith('[removed]') or \
        selftext.startswith('[deleted]') or \
        selftext.startswith('http') or \
        (url and not url.startswith('https://www.reddit.com/r/')):
        return False
    else:
        thumbnail = sub_json['thumbnail']
        assert not (thumbnail and thumbnail.startswith('http://thumbs.reddit.com')), \
            f"Unexpected thumbnail: {thumbnail}"
        return True
    
def load_submissions(submissions_jsons, min_sub_score, max_text_len):
    ''' Load submissions with score above threshold. '''
    submissions = {}

    for sub_json in submissions_jsons:
        score = sub_json['score']
        title = sub_json['title'].strip()
        selftext = sub_json['selftext'].strip()
        url = sub_json['url']
        if valid_submission(sub_json, score, title, selftext, url, min_sub_score, max_text_len):
            sub = {
                'title': title,
                'selftext': selftext,
                'subreddit': sub_json['subreddit'],
                'url': url,
                'score': score,
                'top_child': {'score': 0}
            }
            id = sub_json['id']
            submissions[id] = sub

    return submissions

def load_comments(comments_jsonl, min_com_score, max_text_len):
    ''' Load comments with score above threshold. '''
    comments = {}

    for comment in comments_jsonl:
        try:
            score = comment['score']
            if score >= min_com_score:
                body = comment['body'].strip()
                if body != '[deleted]' and body != '[removed]' and 1 < len(body) < max_text_len:
                    id = comment['id']
                    comment = {
                        'body': body,
                        'score': score,
                        'parent_id': comment['parent_id'][3:],  # remove 'tX_' prefix
                        'top_child': {'score': 0}
                    }
                    comments[id] = comment
        except Exception as e:
            print(f"ERROR: {e}")
            pass

    return comments

def create_threads(submissions_jsonl, comments_jsonl, *, 
                   min_sub_score=10, min_com_score=5, max_text_len=256):
    # Load submissions and comments.
    submissions = load_submissions(submissions_jsonl, min_sub_score, max_text_len)
    all_elem = submissions.copy()
    comments = load_comments(comments_jsonl, min_com_score, max_text_len)
    all_elem.update(comments)

    # Attach comments to corresponding parents, if they have better score.
    for comment in comments.values():
        parent = all_elem.get(comment['parent_id'])
        if parent is not None:
            if comment['score'] > parent['top_child']['score']:
                parent['top_child'] = comment

    # Return only submissions with comments.
    return {id: sub for id, sub in submissions.items() if sub['top_child']['score'] > 0}

def print_threads(threads):
    def print_children(comment, level):
        indent = '\t' * level
        print(f"{indent}{comment['score']} | {comment['body']}")
        top_child = comment['top_child']
        if top_child['score'] > 0:
            print_children(top_child, level + 1)

    print('URL\nScore | Text | Subtext\n\tChildren...')
    for sub in threads.values():
        print(f"\n{sub['url']}\n{sub['score']} | {sub['title']} | {sub['selftext']}")
        print_children(sub['top_child'], 1)

def submission2dict(sub):
    def comments2json(comment):
        if comment['score'] > 0:
            return [{"from": "gpt", "value": comment['body']}] + comments2json(comment['top_child'])
        else:
            return []
    
    title = sub['title']
    selftext = sub['selftext']
    sub_text = title if selftext == '' else f"{title}\n{selftext}" 
    return {"conversations": [{"from": "human", "value": sub_text}] + 
            comments2json(sub['top_child']), "subreddit": sub["subreddit"], "url": sub["url"]}

def generate_json(submissions):
    for sub in submissions.values():
        yield submission2dict(sub)

# Test all.
if __name__ == "__main__":
    # Test on one subrredit file
    root_path = '/home/zel/ml-projects/HUMOR/Reddit-data/'
    submissions_path = root_path + 'FollowThePunchline_submissions/FollowThePunchline_submissions.jsonl'
    comments_path = root_path + 'FollowThePunchline_comments/FollowThePunchline_comments.jsonl'
    threads = create_threads(
        load_jsonl(submissions_path), 
        load_jsonl(comments_path))

    import itertools
    slice = dict(itertools.islice(threads.items(), 3))
    print_threads(slice)

    # Test submission2dict on the previously created slice.
    for sub in slice.values():
        print(submission2dict(sub))

    # Write JSON lines with the threads.
    save_jsonl(root_path + 'out/threads-test.jsonl', generate_json(threads))
