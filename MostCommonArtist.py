import csv
import sys
from collections import Counter

#    Increase CSV field size limit (for large files)
csv.field_size_limit(sys.maxsize)

#    Initialize counters
line_no = 0
artist_counts = Counter()

#    Open and read CSV file
with open('ds29GB.csv', newline='', encoding='utf-8') as csvfile:
    filereader = csv.reader(csvfile, delimiter=",", quotechar='"')

    for row in filereader:
        line_no += 1
        if line_no % 10000 == 0:
            print(f"Processed {line_no} rows...")

        #    Extract artist name
        artist = row[2].strip()

        #    Update count
        if artist:
            artist_counts[artist] += 1

#    Find the most common artist
most_common_artist, most_common_count = artist_counts.most_common(1)[0]

#    Print results
print(f"\nTotal Records Processed: {line_no}")
print(f"Most Common Artist: {most_common_artist} ({most_common_count} songs)")


# Trying to find the name of the artist most common in the dataset

# 2min 12.9sec
# Total Records Processed: 5913412
# Most Common Artist: Genius Romanizations (16573 songs)