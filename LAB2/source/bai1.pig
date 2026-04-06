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
    REPLACE(review, '-', '') AS review,
    category,
    aspect,
    sentiment;

-- tokenize
words = FOREACH data GENERATE
    FLATTEN(TOKENIZE(review)) AS word,
    category,
    aspect,
    sentiment;

-- load stopwords
stopwords = LOAD '/user/ngochan1211/stopwords.txt' USING PigStorage() AS (word: chararray);

-- remove stopwords
joined = JOIN words BY word LEFT OUTER, stopwords BY word;
filtered = FILTER joined BY stopwords::word IS NULL;

words_clean = FOREACH filtered GENERATE
    words::word AS word,          
    words::category AS category,  
    words::aspect AS aspect,       
    words::sentiment AS sentiment; 

-- output
STORE words_clean INTO 'output_bai1' USING PigStorage(',');

-- test
samples = LIMIT words_clean 20;
DUMP samples;
