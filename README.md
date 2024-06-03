# reddit-filter

This project is designed to filter the "humor-chains" dataset from Hugging Face, a machine-filtered collection of the most upvoted Reddit submissions and their replies from humor-related subreddits:
https://huggingface.co/datasets/ZSvedic/humor-chains

## Dataset Creation Process

The dataset is created using the following procedure:
1. **Source Data**: Used older Reddit dump [Subreddit comments/submissions 2005-06 to 2023-12](https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10) instead of Reddit's API due to a more restrictive license (see [2023 Reddit API controversy](https://en.wikipedia.org/wiki/2023_Reddit_API_controversy)).
2. **Subreddit Selection**: Downloaded a portion of subreddits from the [List of funny subreddits](https://www.reddit.com/r/redditlists/comments/128ayc/list_of_funny_subreddits/).
3. **Filtering Submissions**: Python script filtered .jsonl files inside .zst to include only the most upvoted thread for each submission:
   - Score >= 10 upvotes.
   - Total length < 256 characters.
   - Title is not "[deleted by user]".
   - Selftext is not "[removed]" or "[deleted]".
   - Links to itself (Reddit link) and not an image, video, or external link.
   - Doesn't start with a link.
   - Doesn't have a thumbnail.
4. **Filtering Comments**: Filtered comments satisfy:
   - Score >= 5 upvotes.
   - Total length < 256 characters.
   - Only top upvoted reply at each reply level is included.
5. **Humor Detection**: All filtered threads were further filtered with a local model for humor detection: [Humor-Research/humor-detection-comb-23](https://huggingface.co/Humor-Research/humor-detection-comb-23/tree/main), selected because it performed the best on the [lm233/humor_train](https://huggingface.co/datasets/lm233/humor_train?row=9) dataset. Threads not detected as funny were removed.
