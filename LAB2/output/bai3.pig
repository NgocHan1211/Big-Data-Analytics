raw_data = LOAD '/user/ngochan1211/hotel-review.csv' 
USING PigStorage(';') AS (
    id:int, 
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

-- Làm sạch sentiment
data = FOREACH raw_data GENERATE
    aspect,
    LOWER(TRIM(sentiment)) AS sentiment;

-- Xử lý Positive
pos_filter = FILTER data BY sentiment == 'positive';
pos_group = GROUP pos_filter BY aspect;
pos_count = FOREACH pos_group GENERATE
    group AS aspect,
    COUNT(pos_filter) AS cnt;

pos_all = GROUP pos_count ALL;
top_pos = FOREACH pos_all {
    result = TOP(1, 1, pos_count);
    GENERATE FLATTEN(result);
};

-- Xử lý Negative
neg_filter = FILTER data BY sentiment == 'negative';
neg_group = GROUP neg_filter BY aspect;
neg_count = FOREACH neg_group GENERATE
    group AS aspect,
    COUNT(neg_filter) AS cnt;

neg_all = GROUP neg_count ALL;
top_neg = FOREACH neg_all {
    result = TOP(1, 1, neg_count);
    GENERATE FLATTEN(result);
};

STORE top_pos INTO '/user/ngochan1211/output_bai3_top_pos' USING PigStorage(',');
STORE top_neg INTO '/user/ngochan1211/output_bai3_top_neg' USING PigStorage(',');

DUMP top_pos;
DUMP top_neg;