import csv
import sys
import ast
from collections import Counter

# Increase CSV field size limit (for large files)
csv.field_size_limit(sys.maxsize)

# Initialize counters
line_no = 0
main_artist_counts = Counter()
feature_artist_counts = Counter()
overall_artist_counts = Counter()
errorCount=0

# Open and read CSV file
with open('ds29GB.csv', newline='', encoding='utf-8') as csvfile:
    filereader = csv.reader(csvfile, delimiter=",", quotechar='"')

    for row in filereader:
        line_no += 1
        if line_no % 10000 == 0:
            print(f"Processed {line_no} rows...")

        # Extract main artist
        artist = row[2].strip()

        # Update main artist count
        if artist:
            main_artist_counts[artist] += 1
            overall_artist_counts[artist] += 1

        # Extract and parse features
        feature_data = row[5].strip()
        if feature_data.startswith("{") and feature_data.endswith("}"):
            try:
                # Convert to list by replacing curly braces with brackets
                feature_list = ast.literal_eval(feature_data.replace("{", "[").replace("}", "]"))
                for featured_artist in feature_list:
                    featured_artist = featured_artist.strip()
                    if featured_artist:
                        feature_artist_counts[featured_artist] += 1
                        overall_artist_counts[featured_artist] += 1
            except Exception as e:
                errorCount+=1

# Find most common in each category
most_common_main = main_artist_counts.most_common(1)[0]
most_common_feature = feature_artist_counts.most_common(1)[0]
most_common_overall = overall_artist_counts.most_common(1)[0]

# Print results
print(f"\nTotal Records Processed: {line_no}")
print(f"Most Common Main Artist: {most_common_main[0]} ({most_common_main[1]} songs)")
print(f"Most Common Featured Artist: {most_common_feature[0]} ({most_common_feature[1]} features)")
print(f"Most Common Artist Overall: {most_common_overall[0]} ({most_common_overall[1]} appearances)")


# Total Records Processed: 5913412
# Most Common Main Artist: Genius Romanizations (16573 songs)
# Most Common Featured Artist: Genius Brasil Traduções (8842 features)
# Most Common Artist Overall: Genius Romanizations (16573 appearances)

# 3min 46.8 sec