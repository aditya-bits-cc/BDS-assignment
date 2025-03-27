#!/usr/bin/python3
import sys
import json
import re
from datetime import datetime

# Constants
FALSE_NEWS_LABELS = {"FALSE", "PANTS ON FIRE"}
TRUE_NEWS_LABELS = {"TRUE", "MOSTLY-TRUE"}

# Load stop words
with open("stop_words.json") as f:
    STOP_WORDS = set(json.load(f)["stop_words"])

def extract_month_year(date_str):
    """Extract month and year from a date string in MM/DD/YYYY format."""
    date = datetime.strptime(date_str.strip(), '%m/%d/%Y')
    return date.month, date.year

def tokenize_and_filter_stop_words(text):
    """Tokenize text into words, convert to lowercase, and remove stop words."""
    words = re.findall(r'\b[a-zA-Z]+\b(?:\'[a-zA-Z]+)?', text.lower())
    return (word for word in words if word not in STOP_WORDS and len(word) > 1)

# Process each line from stdin
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Parse JSON line
    row = json.loads(line)

    statement = row.get('statement', '')
    source = row.get('statement_source', '').lower()
    originator = row.get('statement_originator', '').lower()
    posted_date = row.get('statement_date', '')
    label = str(row.get('verdict', '')).upper()

    # Emit general metadata
    print(f"label\t{label}")
    print(f"source\t{source}")
    print(f"statement_originator\t{originator}")

    # Extract and emit month-year info
    try:
        month, year = extract_month_year(posted_date)
        month_year = f"{month}-{year}"
        print(f"month_year\t{month_year}")
    except ValueError:
        month_year = None  # Invalid date format; skip month-year output

    # Emit info for false news
    if label in FALSE_NEWS_LABELS:
        print(f"fake_news_source\t{source}")
        print(f"fake_news_statement_originator\t{originator}")
        if month_year:
            print(f"fake_news_month_year\t{month_year}")
        for word in tokenize_and_filter_stop_words(statement):
            print(f"false_news_keyword\t{word}")

    # Emit info for true news
    if label in TRUE_NEWS_LABELS:
        print(f"true_news_source\t{source}")
        print(f"true_news_statement_originator\t{originator}")
