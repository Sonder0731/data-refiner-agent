#!/usr/bin/env bash

set -uo pipefail
trap 'printf "\ninterrupted\n" >&2; exit 130' INT TERM

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
agent_dir="${AGENT_DIR:-/data/projects/data-refiner-agent}"

command -v uv >/dev/null 2>&1 || { echo "uv is not installed" >&2; exit 127; }
[[ -f "$agent_dir/.env" ]] || { echo "missing $agent_dir/.env" >&2; exit 2; }
cd "$agent_dir"

queries=(
  'Read hdfs:///data/cc/000_00000.parquet. Normalize url and split it into scheme, host, path, and query. Filter junk domains, keep only pages where language is en, language_score is at least 0.8, and token_count is between 200 and 5000, then deduplicate by the normalized url. Write the result to hdfs:///data/query_scenarios/20260913_q01_cc_url_quality and report how many rows each stage removes.'
  'Read hdfs:///data/cc/000_00000.parquet. Remove non-printable characters from text and normalize its characters. Remove lines containing web-template noise such as cookie, javascript, or subscribe. Calculate duplicate-line, duplicate-paragraph, duplicate-ngram, most-frequent-ngram character ratios, and text length. Filter garbled text, text shorter than 200 characters, and text with excessive duplication. Write the result to hdfs:///data/query_scenarios/20260913_q02_cc_text_quality.'
  'Read hdfs:///data/cc/000_00000.parquet and process only documents where language is en and token_count is between 100 and 2000. Tokenize text with NLTK and calculate TF-IDF for each document. Write the documents with TF-IDF to hdfs:///data/query_scenarios/20260913_q03_cc_tfidf_docs. Also explode the tokens, use array expansion and counting to calculate corpus-wide term frequencies, and write the top 500 terms to hdfs:///data/query_scenarios/20260913_q03_cc_top_terms.'
  'Read hdfs:///data/cc/000_00000.parquet and first take a random 10 percent sample. Detect language and confidence again from text, remove records with confidence below 0.8, count how many newly detected languages differ from the original language field, and stratify by the newly detected language with at most 200 records per language. Write the result to hdfs:///data/query_scenarios/20260913_q04_cc_language_audit.'
  'Read hdfs:///data/chinese_news/chinese_news.csv with CSV header, multiline, quote, and escape parsing enabled. Remove records with an empty headline or content, convert Traditional Chinese to Simplified Chinese, normalize characters, remove tabs, non-breaking spaces, and non-printable characters, and filter garbled text and extremely rare anomalous characters. Deduplicate exactly by the cleaned headline and write the result to hdfs:///data/query_scenarios/20260913_q05_news_clean.'
  'Read hdfs:///data/chinese_news/chinese_news.csv and correctly parse multiline content. Tokenize content with jieba and use those tokens for MinHash LSH fuzzy deduplication of the news body with a Jaccard threshold of 0.82. Keep one representative record per group, write the result to hdfs:///data/query_scenarios/20260913_q06_news_fuzzy_dedup, and report the row counts before and after deduplication.'
  'Read hdfs:///data/chinese_news/chinese_news.csv. Keep only articles whose content contains the Chinese word for economy, convert date to a Unix timestamp named publish_ts, restrict dates to 2016, group by tag, and return the 20 tags with the highest article counts and their proportions. Do not persist output data.'
  'Read hdfs:///data/chinese_news/chinese_news.csv and take a stratified sample by tag with at most 100 records per tag. Use litellm_mapper on headline and content to return strict JSON with topic, sentiment, and summary fields. Count records by topic and write the classified sample to hdfs:///data/query_scenarios/20260913_q08_news_llm_topics. Do not call the model on the full dataset.'
  'Read hdfs:///data/chinese_news/chinese_news.csv and perform a sensitive-term compliance audit on content. Preserve each matched term and its start and end positions, add a review flag, and do not remove original records. Then mask personally identifiable information in the body, write the audited result to hdfs:///data/query_scenarios/20260913_q09_news_compliance, and summarize the 20 most frequently matched sensitive terms.'
  'Read hdfs:///data/btc_ohlcv_dataset/btcusd_1-min_data.csv with the CSV header enabled. Convert Timestamp to long and Open, High, Low, Close, and Volume to double. Remove records where required fields are null or cannot be converted, filter negative prices and volume, and use Spark SQL to require High to be at least Open, Close, and Low and Low to be at most Open, Close, and High. Deduplicate exactly by Timestamp and write the result to hdfs:///data/query_scenarios/20260913_q10_btc_clean.'
  'Read hdfs:///data/btc_ohlcv_dataset/btcusd_1-min_data.csv and convert fields to their correct numeric types. Use Spark SQL to convert the Unix-seconds Timestamp to a UTC date. For each day, calculate the first Open, highest High, lowest Low, last Close, total Volume, number of trading minutes, and daily return. Sort by date and write the result to hdfs:///data/query_scenarios/20260913_q11_btc_daily_ohlcv.'
  'Read hdfs:///data/btc_ohlcv_dataset/btcusd_1-min_data.csv, convert price fields to double, order by Timestamp, and use Spark SQL to calculate minute return, absolute return, and High-Low range. Calculate the P50, P90, P95, and P99 percentiles for absolute return and range, and report the dates with the most anomalous volatility. Do not persist the full detail dataset.'
  'Read hdfs:///data/credit_card_fraud_detection_dataset/fraudTest.csv with the header enabled and remove the meaningless _c0 column. Deduplicate exactly by trans_num, mask PII in first, last, and street, hash cc_num with SHA-256, remove dob, and keep only fields required for fraud analysis. Write the result to hdfs:///data/query_scenarios/20260913_q13_fraud_masked and confirm that the output contains no original card numbers, names, street addresses, or birth dates.'
  'Read hdfs:///data/credit_card_fraud_detection_dataset/fraudTest.csv. Convert amt to double and is_fraud to integer, remove records where either field is null or invalid, and filter negative amounts. Take a stratified sample by is_fraud with at most 5000 records per class, preserve class-proportion statistics, and write the result to hdfs:///data/query_scenarios/20260913_q14_fraud_balanced_sample.'
  'Read hdfs:///data/credit_card_fraud_detection_dataset/fraudTest.csv. Create a haversine_distance_mapper operator using only the Python standard library to calculate the spherical distance distance_km between the cardholder and merchant from lat, long, merch_lat, and merch_long. Tests must cover nulls, identical coordinates, out-of-range coordinates, and a known distance. After tests pass and the workspace is synchronized, write transactions where distance_km is greater than 200 and amt is greater than 500 to hdfs:///data/query_scenarios/20260913_q15_fraud_long_distance.'
  'Read hdfs:///data/douban_movies/DMSC.csv and correctly handle the UTF-8 BOM, header, quotes, and commas. Convert Movie_Name_CN to Simplified Chinese and normalize its characters, calculate edit distance between the original and normalized movie names, and use the existing chinese_movie_pinyin_mapper in the workspace to generate movie_name_pinyin and movie_name_initials. Deduplicate exactly by the normalized movie name and write the result to hdfs:///data/query_scenarios/20260913_q16_movie_names.'
  'Read hdfs:///data/douban_movies/DMSC.csv. Create a chinese_sentiment_mapper operator using exactly snownlp==0.12.3 to generate a sentiment_score from 0 to 1 for Comment. Tests must cover nulls, empty strings, positive and negative Chinese-language reviews, English, and symbols. Normalize comment characters, keep comments with lengths from 5 to 500, and apply MinHash LSH fuzzy deduplication with a threshold of 0.85. After tests pass and the workspace and conda environment are synchronized, write the result to hdfs:///data/query_scenarios/20260913_q17_movie_sentiment.'
  'Read hdfs:///data/douban_movies/DMSC.csv. Convert Star and Like to numeric values and filter records where Star is outside 1 through 5 or Like is negative. Calculate the P50, P90, and P99 percentiles of Like, then aggregate by Movie_Name_CN to calculate review count, average star rating, total likes, and highly liked review count. Write the 200 most popular movies with at least 100 reviews to hdfs:///data/query_scenarios/20260913_q18_movie_rating_summary.'
  'Read hdfs:///data/job_postings/postings.csv and correctly parse multiline description and skills_desc fields and numerous nulls. Keep only jobs where currency is USD and salary data is valid. Convert salaries to annual_min_salary and annual_max_salary based on HOURLY, WEEKLY, MONTHLY, or YEARLY, preferring a valid normalized_salary. Aggregate job count and average annual salary by location, formatted_experience_level, and formatted_work_type, and write the 200 groups with the highest job counts to hdfs:///data/query_scenarios/20260913_q19_job_salary.'
  'Read hdfs:///data/job_postings/postings.csv and correctly parse multiline fields. Convert remote_allowed to integer, keep only remote jobs whose title contains Data, remove records with empty skills_desc, extract date and time information from description, tokenize skills_desc with NLTK, and calculate TF-IDF. Explode skill terms and count occurrences, exclude English stop words, and write the top 300 skills and their job counts to hdfs:///data/query_scenarios/20260913_q20_remote_data_skills.'
)

run_id="${RUN_ID:-$(date '+%Y%m%d_%H%M%S')}"
log_dir="${LOG_DIR:-$script_dir/logs/query-scenarios/$run_id}"
mkdir -p "$log_dir"
summary="$log_dir/summary.tsv"
printf 'number\tstatus\texit_code\tduration_seconds\tlog\n' > "$summary"

failed=0
for index in "${!queries[@]}"; do
  number="$(printf '%02d' "$((index + 1))")"
  query="${queries[$index]}"
  query_file="$log_dir/$number.query.txt"
  log_file="$log_dir/$number.log"
  printf '%s\n' "$query" > "$query_file"
  started_at="$(date --iso-8601=seconds)"
  started_epoch="$(date +%s)"

  {
    printf 'started_at: %s\n' "$started_at"
    printf 'query_file: %s\n' "$query_file"
    printf 'command: '
    printf '%q ' uv run --env-file .env data-refiner-agent --user-id '@admin:example.test' "$query" --debug
    printf '\n\n'
  } > "$log_file"

  printf '[%s/20] START log=%s\n' "$number" "$log_file"
  uv run --env-file .env data-refiner-agent --user-id '@admin:example.test' "$query" --debug >> "$log_file" 2>&1
  status=$?
  ended_at="$(date --iso-8601=seconds)"
  duration="$(( $(date +%s) - started_epoch ))"

  if (( status == 0 )); then
    result=PASS
  else
    result=FAIL
    failed=$((failed + 1))
  fi
  printf '\nended_at: %s\nexit_code: %d\nduration_seconds: %d\n' "$ended_at" "$status" "$duration" >> "$log_file"
  printf '%s\t%s\t%d\t%d\t%s\n' "$number" "$result" "$status" "$duration" "$log_file" >> "$summary"
  printf '[%s/20] %s exit=%d duration=%ss\n' "$number" "$result" "$status" "$duration"
done

printf 'completed: %d passed, %d failed\nsummary: %s\n' "$((20 - failed))" "$failed" "$summary"
(( failed == 0 ))
