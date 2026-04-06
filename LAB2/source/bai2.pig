-- Load data
data = LOAD '/user/ngochan1211/hotel-review.csv' USING PigStorage(';') AS (
    id: int, 
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- lowercase
data = FOREACH data GENERATE
    LOWER(review) AS review,
    category,
    aspect,
    sentiment;

-- remove punctuation
data = FOREACH data GENERATE
    REPLACE(review, '[0-9,.&@!%+/\\\\?\\\':()>^~*#=\\[\\]{}"-]', '') AS review,
    category,
    aspect,
    sentiment;

data = FOREACH data GENERATE
    REPLACE(review, '-', '') AS review_clean,
    category,
    aspect,
    sentiment;

-- Tokenize
words = FOREACH data GENERATE
    FLATTEN(TOKENIZE(review_clean)) AS word,
    category,
    aspect,
    sentiment;

-- Load stopwords
stopwords = LOAD '/user/ngochan1211/stopwords.txt' USING PigStorage() AS (word);

-- Remove stopwords
joined = JOIN words BY word LEFT OUTER, stopwords BY word;
words_clean = FILTER joined BY stopwords::word IS NULL;

words_clean = FOREACH words_clean GENERATE
    words::word AS word,
    words::category AS category,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

-- 1. Thống kê tần số từ (>500 lần)
words_group = GROUP words_clean BY word;
words_count = FOREACH words_group GENERATE
    group AS word,
    COUNT(words_clean) AS cnt;
words_over_500 = FILTER words_count BY cnt > 500;
STORE words_over_500 INTO '/user/ngochan1211/output_bai2_word_freq' USING PigStorage(',');

-- 2. Thống kê số bình luận theo category
category_group = GROUP data BY category;
category_count = FOREACH category_group GENERATE
    group AS category,
    COUNT(data) AS cnt;
STORE category_count INTO '/user/ngochan1211/output_bai2_category_count' USING PigStorage(',');

-- 3. Thống kê số bình luận theo aspect
aspect_group = GROUP data BY aspect;
aspect_count = FOREACH aspect_group GENERATE
    group AS aspect,
    COUNT(data) AS cnt;
STORE aspect_count INTO '/user/ngochan1211/output_bai2_aspect_count' USING PigStorage(',');