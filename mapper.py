#!/usr/bin/python3
import sys
import json
import re
from datetime import datetime

FALSE_NEWS_LABELS = {"FALSE", "PANTS ON FIRE"}
TRUE_NEWS_NABELS = {"TRUE", "MOSTLY-TRUE"}

# Load the stop words
with open("stop_words.json") as f:
    STOP_WORDS = set((json.load(f)["stop_words"]))
    
# Function to extract month and year
def extract_month_year(date_str):
    date = datetime.strptime(date_str.strip(), '%m/%d/%Y')
    return date.month, date.year

def tokenize_and_filter_stop_words(text):
    # Lowercase and split text into words
    words = re.findall(r'\b[a-zA-Z]+\b(?:\'[a-zA-Z]+)?', text.lower())
    # Filter out stop words
    filtered_words = (word for word in words if word not in STOP_WORDS and len(word) > 1)
    return filtered_words

# Reading input from stdin
for line in sys.stdin:
    line = line.strip()
    # line = line.encode('utf-8').decode('unicode_escape')
    # Parse the JSON object
    row = json.loads(line)
    # for row in reader:
    headline = row['statement']
    source = row['statement_source'].lower()
    statement_originator = row['statement_originator'].lower()
    posted_on = row['statement_date']
    label = str(row['verdict']).upper()

    # Count the labels
    print(f"label\t{label}")

    # Count the sources
    print(f"source\t{source}")
    
    # Count the person who gave the statement
    print(f"statement_originator\t{statement_originator}")
    
    # Extract the month-year for the posted date
    month, year = extract_month_year(posted_on)
    if month and year:
        month_year = f"{month}-{year}"
        print(f"month_year\t{month}-{year}")

    if label in FALSE_NEWS_LABELS:
        print(f"fake_news_source\t{source}") # Print source if news is fake
        print(f"fake_news_statement_originator\t{statement_originator}") # Print statement_originator if news is fake
        print(f"fake_news_month_year\t{month_year}")  # Print month-year Of fake news
        
        # Process the headline for keywords
        words = tokenize_and_filter_stop_words(headline)
        for word in words:
            print(f"false_news_keyword\t{word}")

    if label in TRUE_NEWS_NABELS:
        print(f"true_news_source\t{source}") # Source name with true or mostly-true news
        print(f"true_news_statement_originator\t{statement_originator}") # Print statement_originator if news is true or mostly-true
