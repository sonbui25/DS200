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

--1. Thống kê tần số xuất hiện của các từ. Chỉ ra các từ xuất hiện trên 500 lần.
word_group = GROUP clean_data BY word;
word_freq = FOREACH word_group GENERATE group AS word, COUNT(clean_data) AS cnt;
word_freq_higher_500 = FILTER word_freq BY cnt > 500;
word_freq_higher_500_sorted = ORDER word_freq_higher_500 BY cnt DESC;
STORE word_freq_higher_500_sorted INTO '/user/sonbui13/lab2/output/ex2_word' USING PigStorage('\t');

-- Load raw data
raw_data = LOAD '/user/sonbui13/lab2/input/hotel-review.csv' USING PigStorage(';')
    AS (id:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

--2. Thống kê số bình luận theo từng phân loại (category).
category_group = GROUP raw_data BY category;
category_freq = FOREACH category_group GENERATE group AS category, COUNT(raw_data) AS cnt;
category_freq_sorted = ORDER category_freq BY cnt DESC;
STORE category_freq_sorted INTO '/user/sonbui13/lab2/output/ex2_category' USING PigStorage('\t');

--3. Thống kê số bình luận theo từng khía cạnh đánh giá (aspect).
aspect_group = GROUP raw_data BY aspect;
aspect_freq = FOREACH aspect_group GENERATE group AS aspect, COUNT(raw_data) AS cnt;
aspect_freq_sorted = ORDER aspect_freq BY cnt DESC;
STORE aspect_freq INTO '/user/sonbui13/lab2/output/ex2_aspect' USING PigStorage('\t');

--In ra màn hình
DUMP word_freq_higher_500_sorted;
DUMP category_freq_sorted;
DUMP aspect_freq_sorted;
