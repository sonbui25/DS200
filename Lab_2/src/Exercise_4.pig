--Load data từ exercise 1
clean_data = LOAD '/user/sonbui13/lab2/output/ex1/part-r-00000'
USING PigStorage('\t')
AS (
    id:int,
    category:chararray,
    aspect:chararray,
    sentiment:chararray,
    word:chararray
);

--2. Lấy top 5 từ tích cực cho mỗi category
positive_data = FILTER clean_data BY sentiment == 'positive';
word_cnt = FOREACH (GROUP positive_data BY (category, word))
    GENERATE 
        group.category AS category,
        group.word AS word,
        COUNT(positive_data) AS freq;

by_category = GROUP word_cnt BY category;

top_5_positive_result = FOREACH by_category {
    sorted = ORDER word_cnt BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5) AS (category, word, freq);
};

STORE top_5_positive_result INTO '/user/sonbui13/lab2/output/ex4_top_positive_category' USING PigStorage('\t');

--3. Lấy top 5 từ tiêu cực cho mỗi category
negative_data = FILTER clean_data BY sentiment == 'negative';
word_cnt = FOREACH (GROUP negative_data BY (category, word))
    GENERATE 
        group.category AS category,
        group.word AS word,
        COUNT(negative_data) AS freq;

by_category = GROUP word_cnt BY category;

top_5_negative_result = FOREACH by_category {
    sorted = ORDER word_cnt BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5) AS (category, word, freq);
};

STORE top_5_negative_result INTO '/user/sonbui13/lab2/output/ex4_top_negative_category' USING PigStorage('\t');
--4. In kết quả
DUMP top_5_positive_result;
DUMP top_5_negative_result;