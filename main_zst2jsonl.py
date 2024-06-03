import reddit_json
import zst_handling
import os

def zst2jsonl(root='/home/zel/ml-projects/HUMOR/Reddit-data/'):
    for sub_file, com_file, out_file in folder_get_file_triplets(root):
        print(f'\nSubmissions: {sub_file}\n   Comments: {com_file}')
        if os.path.exists(out_file):
            print(f"The file already exists, skipping {out_file}.")
        else:
            print(f"   Creating: {out_file}...")
            # Create threads from the submissions and comments.
            threads = reddit_json.create_threads(
                zst_handling.read_zst_file(sub_file), 
                zst_handling.read_zst_file(com_file))
            # Write JSON lines with the threads.
            reddit_json.save_jsonl(out_file, reddit_json.generate_json(threads))

def folder_get_file_triplets(root_folder, *, 
                            suffix_submissions = '_submissions.zst', 
                            suffix_comments = '_comments.zst'):
    ''' Generator that yields triplets of file paths for Reddit's .ZST submissions, 
    comments and output files. '''
    for file in os.listdir(root_folder):
        if file.endswith('submissions.zst'):
            sub_file_path = os.path.join(root_folder, file)
            com_file_path = sub_file_path.replace(suffix_submissions, suffix_comments)
            out_file_name = file.replace(suffix_submissions, '_threads.jsonl')
            out_file_path = os.path.join(root_folder, '1-threads', out_file_name)
            yield sub_file_path, com_file_path, out_file_path

if __name__ == "__main__":
    zst2jsonl()
