#!/usr/bin/python3
import sys

# Dicts for keeping the count
label_counts = {}
source_counts = {}
statement_originator_counts = {}
source_fake_news_count = {}
source_true_news_count = {}
statement_originator_fake_news_count = {}
statement_originator_true_news_count = {}
month_year_counts = {}
month_year_fake_news_count = {}
month_year_percentage_fake_news = {}
false_news_keyword_counts = {}

# Reading the mapper output from stdin
for line in sys.stdin:
    line = line.strip()
    
    key, value = line.split('\t', 1)
    
    # 1. Count labels
    if key == "label":
        if value in label_counts:
            label_counts[value] += 1
        else:
            label_counts[value] = 1
    
    # 2. Count sources
    elif key == "source":
        if value in source_counts:
            source_counts[value] += 1
        else:
            source_counts[value] = 1
            
    # 2. Count statement_originator
    elif key == "statement_originator":
        if value in statement_originator_counts:
            statement_originator_counts[value] += 1
        else:
            statement_originator_counts[value] = 1
            
    # 3. Count headlines posted per month-year
    elif key == "month_year":
        if value in month_year_counts:
            month_year_counts[value] += 1
        else:
            month_year_counts[value] = 1

    # 4. Sources with the most fake news (False, Pants on Fire)
    elif key == "fake_news_source":
        if value in source_fake_news_count:
            source_fake_news_count[value] += 1
        else:
            source_fake_news_count[value] = 1

    # 5. Sources with the most true news (True, Mostly-True)
    elif key == "true_news_source":
        if value in source_true_news_count:
            source_true_news_count[value] += 1
        else:
            source_true_news_count[value] = 1
    
    # statement_originator with most fake news     
    elif key == "fake_news_statement_originator":
        if value in statement_originator_fake_news_count:
            statement_originator_fake_news_count[value] += 1
        else:
            statement_originator_fake_news_count[value] = 1
            
    # 5. statement_originator with the most true news (True, Mostly-True)
    elif key == "true_news_statement_originator":
        if value in statement_originator_true_news_count:
            statement_originator_true_news_count[value] += 1
        else:
            statement_originator_true_news_count[value] = 1

    # 6. Track fake news count per month-year
    elif key == "fake_news_month_year":
        if value in month_year_fake_news_count:
            month_year_fake_news_count[value] += 1
        else:
            month_year_fake_news_count[value] = 1
            
    # 7. Fake news keyword count
    elif key == "false_news_keyword":
        if value in false_news_keyword_counts:
            false_news_keyword_counts[value] += 1
        else:
            false_news_keyword_counts[value] = 1

# Count of each label e.g. TRUE, FALSE, MOSTLY-TRUE etc
print("\nVeracity count:")
for label, count in label_counts.items():
    print(f"{label} -> {count}")

# Top 3 sources with most absolute false news
source_fake_news_sorted = sorted(source_fake_news_count.items(), key=lambda x: x[1], reverse=True)
print("\nTop 3 sources with false statements:")
for i, (source, count) in enumerate(source_fake_news_sorted[:3]):
    print(f"{source}: {count} fake news")

# Top 3 Sources with most TRUE or MOSTLY-TRUE news
source_true_news_sorted = sorted(source_true_news_count.items(), key=lambda x: x[1], reverse=True)
print("\nTop 3 sources with True or Mostly-true statements:")
for i, (source, count) in enumerate(source_true_news_sorted[:3]):
    print(f"{source}: {count} true news")

# Top 3 originators with most absolute false news
source_fake_news_sorted = sorted(statement_originator_fake_news_count.items(), key=lambda x: x[1], reverse=True)
print("\nTop 3 originators with absolute false statements:")
for i, (source, count) in enumerate(source_fake_news_sorted[:3]):
    print(f"{source}: {count} fake news")

# Top 3 originators with most TRUE or MOSTLY-TRUE news
source_true_news_sorted = sorted(statement_originator_true_news_count.items(), key=lambda x: x[1], reverse=True)
print("\nTop 3 originators with True or Mostly-true statements:")
for i, (source, count) in enumerate(source_true_news_sorted[:3]):
    print(f"{source}: {count} true news")

# Month-year with most fake news
month_year_fake_news_sorted = sorted(month_year_fake_news_count.items(), key=lambda x: x[1], reverse=True)
print("\nTop 3 Month-Year with the highest overall false news count:")
for i, (month_year, count) in enumerate(month_year_fake_news_sorted[:3]):
    print(f"{month_year}: {count} fake news out of {month_year_counts.get(month_year, 0)} news")

# Calculate the month-year with the most percentage of fake news
for month_year in month_year_fake_news_count:
    # Get the total headlines for this month-year
    total_headlines = month_year_counts.get(month_year, 0)
    fake_news_count = month_year_fake_news_count.get(month_year, 0)

    # Calculate the percentage of fake news for this month-year
    if total_headlines > 10:
        fake_news_percentage = (fake_news_count / total_headlines) * 100
        month_year_percentage_fake_news[month_year] = fake_news_percentage

# Sort month-year by the percentage of fake news
sorted_month_year_percentage_fake_news = sorted(month_year_percentage_fake_news.items(), key=lambda x: x[1], reverse=True)

# month-year with the highest percentage of fake news
print("\nTop 3 Month-Year with the highest percentage of false news (considering months with minimum of 10 total news):")
for month_year, percentage in sorted_month_year_percentage_fake_news[:3]:
    print(f"{month_year}: {percentage:.2f}% fake news")
    
print("\nTop 5 Keywords for False News:")
sorted_keywords = sorted(false_news_keyword_counts.items(), key=lambda x: x[1], reverse=True)
for i, (keyword, count) in enumerate(sorted_keywords[:5]):
    print(f"{keyword}: {count}")
