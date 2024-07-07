import os
import zstandard as zstd

def decompress_zst_file(input_file, output_file):
    dctx = zstd.ZstdDecompressor()
    with open(input_file, 'rb') as ifh, open(output_file, 'wb') as ofh:
        dctx.copy_stream(ifh, ofh)

# Directory containing the .tsv.zst files
directory = 'zst_folder'
destination = 'tsv_folder'

for filename in os.listdir(directory):
    if filename.endswith('.tsv.zst'):
        input_file = os.path.join(directory, filename)
        output_file = os.path.join(destination, filename[:-4])  # Remove the .zst extension
        decompress_zst_file(input_file, output_file)
