import os
import argparse
from tqdm import tqdm
from typing import Callable
import transformers as trans # type: ignore
from reddit_json import load_jsonl, save_jsonl

def main():
    # Define and parse command-line arguments.
    parser = argparse.ArgumentParser(
        description="Filter Reddit's JSONL files using a machine learning model.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--files', nargs=2, metavar=('IN_FILE', 'OUT_FILE'), 
                       help='Input and output file for filtering.')
    group.add_argument('--folders', nargs=2, metavar=('IN_FOLDER', 'OUT_FOLDER'), 
                       help='Input and output folder for filtering.')
    args = parser.parse_args()

    # Get pipeline.
    pipe = get_humor_research_pipeline()
    
    # Run filtering on a file or on an entire folder.
    if args.files:
        in_file, out_file = args.files
        ml_filter(pipe, in_file, out_file)
    elif args.folders:
        in_folder, out_folder = args.folders
        for filename in os.listdir(in_folder):
            if filename.endswith('.jsonl'):
                in_file = os.path.join(in_folder, filename)
                out_file = os.path.join(out_folder, filename)
                ml_filter(pipe, in_file, out_file)

def ml_filter(pipe, in_file_path, out_file_path):
    if os.path.exists(out_file_path):
        print(f"The file already exists, skipping {out_file_path}.")
        return

    print(f"***** Creating: {out_file_path}...")
    
    # Load input file and extract texts.
    json_lines = list(load_jsonl(in_file_path))
    texts = ["POST: " + "\nREPLY: ".join([d['value'] for d in jsonl['conversations']])
        for jsonl in json_lines]
    
    # Init progress bar, split into chunks, and run the pipeline on each chunk.
    results = []
    with tqdm(total=len(texts)) as pbar:
        for i in range(0, len(texts), 50):
            chunk = texts[i:i+50]
            results.extend(pipe(chunk))
            pbar.update(len(chunk))
    
    # Filter funny texts and report on non-funny ones.
    funny = []
    removed = 0
    for t, r, jsonl in zip(texts, results, json_lines):
        if r == 1:
            funny.append(jsonl)
        else:
            print(f"--- Not funny:\n{t}\n{jsonl['url']}")
            removed += 1
    
    # Save the filterted lines.
    save_jsonl(out_file_path, funny)
    print(f"Removed {removed} lines, output file now has {len(funny)} lines.")

def get_humor_research_pipeline(*, 
                                model_name: str = "Humor-Research/humor-detection-comb-23",
                                tokenizer_name: str = "roberta-base") -> Callable:
    model = trans.RobertaForSequenceClassification.from_pretrained(model_name)
    tokenizer = trans.RobertaTokenizerFast.from_pretrained(
        tokenizer_name, max_length=512, truncation=True)
    text_pipe = trans.TextClassificationPipeline(
        model=model, tokenizer=tokenizer, max_length=512, truncation=True)
    binary_pipe = lambda x: [1 if h['label']=='LABEL_1' else 0 
                             for h in text_pipe(x)]
    return binary_pipe

if __name__ == "__main__":
    main()