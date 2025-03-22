import csv
import sys
import csv

line_no=0
csv.field_size_limit(sys.maxsize)
max_lines=0
max_song=""
with open('ds29GB.csv', newline='') as csvfile:
    filereader = csv.reader(csvfile, delimiter=",", quotechar='"')
    for row in filereader:
        line_no+=1
        # if line_no%10000==0:
        #     print(row)
        lyrics=row[6]
        title=row[0]
        lines = lyrics.split(' ')#Using spaces can use \n
        no_of_lines=len(lines)
        if no_of_lines > max_lines:
            max_lines=no_of_lines
            max_song=title
        # if line_no > 50000:
        #       break
print(line_no)
print (max_lines)
print(max_song)
       
# Simple example of finding the song with the most lines in it using Highlevel language (Python)
#     Result using \n as split:
#         Current longest: Patient Protection and Affordable Care Act ObamaCare
#         Time taken: 4min 18.8sec
#         Line Count:29251
#         Total rows searched: 5913412
#     Result using " " as split:
#         Current longest: Patient Protection and Affordable Care Act ObamaCare
#         Time taken: 5min 33.3sec
#         Space Count:365904
#         Total rows searched: 5913412

# Taking this as the longest as it simplity goes through them all

# SQL Query: SELECT title, 
#                 LENGTH(lyrics) - LENGTH(REPLACE(lyrics, ' ', '')) + 1 AS word_count
#             FROM songs
#             ORDER BY word_count DESC
#             LIMIT 1;
