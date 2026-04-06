-- Load dữ liệu 
words_clean = LOAD '/user/ngochan1211/output_bai1' USING PigStorage(',') AS (
    word: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Lọc từ positive
positive_words = FILTER words_clean BY sentiment == 'positive';

-- Nhóm theo (category, word) 
pos_words_group = GROUP positive_words BY (category, word);

-- Tính số lượng 
pos_words_count = FOREACH pos_words_group GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(positive_words) AS cnt;

-- Nhóm theo category 
pos_category_all = GROUP pos_words_count BY category;

top_5_positive_words = FOREACH pos_category_all GENERATE 
    group AS category, 
    FLATTEN(TOP(5, 2, pos_words_count)) AS (category_dummy, word, cnt);

-- Loại bỏ cột trùng lặp 
final_positive = FOREACH top_5_positive_words GENERATE category, word, cnt;

-- Lọc từ negative
negative_words = FILTER words_clean BY sentiment == 'negative';

-- Nhóm theo (category, word)
neg_words_group = GROUP negative_words BY (category, word);

-- Tính số lượng 
neg_words_count = FOREACH neg_words_group GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(negative_words) AS cnt;

-- Nhóm theo category 
neg_category_all = GROUP neg_words_count BY category;

top_5_negative_words = FOREACH neg_category_all GENERATE 
    group AS category, 
    FLATTEN(TOP(5, 2, neg_words_count)) AS (category_dummy, word, cnt);

-- bỏ cột trùng lặp
final_negative = FOREACH top_5_negative_words GENERATE category, word, cnt;

STORE final_positive INTO '/user/ngochan1211/output_bai4_positive_top5' USING PigStorage(',');
STORE final_negative INTO '/user/ngochan1211/output_bai4_negative_top5' USING PigStorage(',');

DUMP final_positive;
DUMP final_negative;