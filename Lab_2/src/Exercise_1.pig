-- 1. Read data
raw = LOAD '/user/sonbui13/lab2/input/hotel-review.csv' USING PigStorage(';')
    AS (id:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);
-- 2. Lowercase
normalized = FOREACH raw GENERATE
    id, LOWER(comment) AS comment_lc, category, aspect, sentiment;

-- 3. 
tokenized = FOREACH normalized GENERATE 
    id, category, aspect, sentiment, FLATTEN(TOKENIZE(comment_lc)) AS word;

--4 Read stopwords files
stops = LOAD '/user/sonbui13/lab2/input/stopwords.txt' USING PigStorage('\n') AS (sw:chararray);

-- anti-join pattern: giữ từ không nằm trong stopwords
join_tokenized_stop_words = JOIN tokenized BY word LEFT OUTER, stops BY sw;
clean_words = FILTER join_tokenized_stop_words BY (word MATCHES '.*\\p{L}.*') AND NOT (word MATCHES '.*\\p{N}.*');
final_words = FOREACH clean_words GENERATE id, category, aspect, sentiment, word;

-- 5. Save results
DUMP final_words;
-- 6. Save results
STORE final_words INTO '/user/sonbui13/lab2/output/ex1' USING PigStorage('\t');