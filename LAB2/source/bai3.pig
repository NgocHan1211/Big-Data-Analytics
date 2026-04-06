-- Load dữ liệu 
words_clean = LOAD '/user/ngochan1211/output_bai1' USING PigStorage(',') AS (
    word: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- negative
negative_data = FILTER words_clean BY sentiment == 'negative';
negative_group = GROUP negative_data BY aspect;
negative_counts = FOREACH negative_group GENERATE
    group AS aspect,
    COUNT(negative_data) AS cnt;

-- Gom tất cả lại để tìm Top 1
negative_all = GROUP negative_counts ALL;
top_negative = FOREACH negative_all {
    result = TOP(1, 1, negative_counts); 
    GENERATE FLATTEN(result);
};

-- positive
positive_data = FILTER words_clean BY sentiment == 'positive';
positive_group = GROUP positive_data BY aspect;
positive_counts = FOREACH positive_group GENERATE
    group AS aspect,
    COUNT(positive_data) AS cnt;

-- Gom tất cả lại để tìm Top 1
positive_all = GROUP positive_counts ALL;
top_positive = FOREACH positive_all {
    result = TOP(1, 1, positive_counts);
    GENERATE FLATTEN(result);
};

-- lưu
STORE top_negative INTO '/user/ngochan1211/output_bai3_negative' USING PigStorage(',');
STORE top_positive INTO '/user/ngochan1211/output_bai3_positive' USING PigStorage(',');

DUMP top_negative;
DUMP top_positive;