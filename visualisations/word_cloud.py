#!usr/bin/env python3

import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords

# Set stop words
stop_words = set(stopwords.words('english'))

# Assuming your CSV data is in a file named 'data.csv'
# Read the CSV file into a pandas DataFrame
data = pd.read_csv('word_counts.tsv', header=None, names=['word', 'count'], sep='\t')

# Remove stopwords from the DataFrame
data = data[~data['word'].isin(stop_words)]

# Select the top 100 words
top_words = data.sort_values(by='count', ascending=False).head(100)

# Create a dictionary from the filtered DataFrame for word cloud generation
word_count_dict = dict(zip(data['word'], data['count']))

# Create a WordCloud object
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_count_dict)

# Display the word cloud using matplotlib
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()