import csv
import sys
import re
from collections import Counter, defaultdict

# Set max CSV field size
csv.field_size_limit(sys.maxsize)

# Data storage
genre_word_counts = defaultdict(lambda: Counter())  # { genre: {word: count} }
line_no = 0

# Open and read the CSV file
with open('ds29GB.csv', newline='', encoding='utf-8') as csvfile:
    filereader = csv.reader(csvfile, delimiter=",", quotechar='"')

    for row in filereader:
        line_no += 1

        # Extract genre and lyrics
        genre = row[1].strip().lower()  # Normalize genre to lowercase
        lyrics = row[6]

        # Extract only 5-letter words
        words = re.findall(r'\b\w{5}\b', lyrics.lower())

        # Update word counts for the genre
        genre_word_counts[genre].update(words)

# Find the most common 5-letter word in each genre, excluding "verse"
genre_most_common_word = {}
for genre, word_counts in genre_word_counts.items():
    word_counts.pop("verse", None)  # Exclude "verse" if it exists

    if word_counts:
        genre_most_common_word[genre] = word_counts.most_common(1)[0][0]
    else:
        genre_most_common_word[genre] = "No 5-letter words found"

# Output results
print("\nMost Common 5-Letter Word in Each Genre (excluding 'verse'):")
for genre, word in genre_most_common_word.items():
    print(f"{genre}: {word}")
