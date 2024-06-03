# From: https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py

import zstandard
import orjson
import os
import tqdm

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	# Copilot: The reason for chunking here is to handle cases where the data might not be 
    # decodable due to being split across chunks.
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		print(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		# Copilot: The reason for chunking here is to handle large files that might not fit 
        # into memory if read all at once. 
		# Copilot: The buffer variable in this code is used to handle the case where a line of text 
        # might be split across two chunks read from the file.
        # When reading a file in chunks, it's possible that the last line in a chunk might not be 
		# a complete line. The remaining part of the line would be at the start of the next chunk.
        # To handle this, the code keeps the last line of each chunk in the buffer. When the next 
		# chunk is read, it's prepended with the buffer to form a complete line.
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			# Copilot speedup tip: get the file position once per chunk.
			file_pos = file_handle.tell()

			for line in lines[:-1]:
				yield line, file_pos

			buffer = lines[-1]

		reader.close()

def read_zst_file(file_path, *, report_errors=True):
	with tqdm.tqdm(total=os.path.getsize(file_path), unit='B', unit_scale=True) as pbar:
		for line, file_bytes_processed in read_lines_zst(file_path):
			try:
				yield orjson.loads(line)
			except (KeyError, orjson.JSONDecodeError) as err:
				if report_errors:
					print(err)
			pbar.update(file_bytes_processed - pbar.n)

# Test all.
if __name__ == "__main__":
	file_path = "/home/zel/ml-projects/HUMOR/Reddit-data/FollowThePunchline_comments.zst"
	processed_lines = 0

	for json_line in read_zst_file(file_path):
		if processed_lines % 10000 == 0:
			print(json_line)
		processed_lines += 1

	print(f"Processed {processed_lines} lines")

