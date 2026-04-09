words_clean = LOAD '/user/ngochan1211/output_bai1' USING PigStorage(',') AS (
    word: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

all_words_group = GROUP words_clean BY (category, word);
all_words_count = FOREACH all_words_group GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(words_clean) AS cnt;

category_all_group = GROUP all_words_count BY category;

top_5_related_words = FOREACH category_all_group GENERATE 
    group AS category, 
    FLATTEN(TOP(5, 2, all_words_count)) AS (category_dummy, word, cnt);
final_related = FOREACH top_5_related_words GENERATE category, word, cnt;

STORE final_related INTO '/user/ngochan1211/output_bai5_related_top5' USING PigStorage(',');

DUMP final_related;