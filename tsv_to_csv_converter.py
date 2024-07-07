import os
import csv
import datetime

def tsv_to_csv(input_file, output_file, delimiter='|', lineterminator='\r\n'):
    with open(input_file, 'r', newline='', encoding='utf-8') as tsv_file:
        tsv_reader = csv.reader(tsv_file, delimiter='\t')
        with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=delimiter, lineterminator=lineterminator)
            for row in tsv_reader:
                csv_writer.writerow(row)
    print(f"Converted {input_file} to {output_file}")

# Directory containing the .tsv files
directory = 'tsv_folder'
destination = 'csv_folder'

print("Started conversion: ", datetime.datetime.now())
for filename in os.listdir(directory):
    if filename.endswith('.tsv'):
        input_file = os.path.join(directory, filename)
        output_file = os.path.join(destination, filename[:-4] + '.csv')  # Change the extension to .csv
        tsv_to_csv(input_file, output_file)
print("Conversion complete: ", datetime.datetime.now())
