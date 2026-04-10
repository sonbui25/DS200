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

-- Gom lại các nhóm theo (category, word)
word_cnt = FOREACH (GROUP clean_data BY (category, word)) 
    GENERATE
        group.category as category,
        group.word as word,
        COUNT(clean_data) as freq;

-- Gom lại theo từng mục category và tiến hành lấy 5 từ liên quan nhất
by_category = GROUP word_cnt BY category;
top_5_relevant_words = FOREACH by_category {
    sorted = ORDER word_cnt BY freq DESC;
    top_5 = LIMIT sorted 5;
    GENERATE FLATTEN (top_5) AS (category, word, freq);
};

STORE top_5_relevant_words INTO '/user/sonbui13/lab2/output/ex5_top_relevant_word_category' USING PigStorage('\t');
DUMP top_5_relevant_words;
