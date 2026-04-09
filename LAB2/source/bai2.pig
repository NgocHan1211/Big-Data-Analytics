-- Load data 
words_clean = LOAD '/user/ngochan1211/output_bai1' USING PigStorage(',') AS (
    word: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Thống kê tần số từ 
words_group = GROUP words_clean BY word;
words_count = FOREACH words_group GENERATE
    group AS word,
    COUNT(words_clean) AS cnt;

words_over_500 = FILTER words_count BY cnt > 500;
STORE words_over_500 INTO '/user/ngochan1211/output_bai2_word_freq' USING PigStorage(',');

-- Load lại file gốc 
raw_data = LOAD '/user/ngochan1211/hotel-review.csv' USING PigStorage(';') AS (
    id: int, 
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Thống kê số bình luận theo category
category_group = GROUP raw_data BY category;
category_count = FOREACH category_group GENERATE
    group AS category,
    COUNT(raw_data) AS cnt; -- Đếm dòng trên data gốc = Số bình luận chuẩn

STORE category_count INTO '/user/ngochan1211/output_bai2_category_count' USING PigStorage(',');

-- Thống kê số bình luận theo aspect
aspect_group = GROUP raw_data BY aspect;
aspect_count = FOREACH aspect_group GENERATE
    group AS aspect,
    COUNT(raw_data) AS cnt;

STORE aspect_count INTO '/user/ngochan1211/output_bai2_aspect_count' USING PigStorage(',');