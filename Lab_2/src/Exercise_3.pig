-- 1. Đọc file đánh giá khách sạn trên HDFS (delimiter ';'), khai báo kiểu từng cột
data = LOAD '/user/sonbui13/lab2/input/hotel-review.csv' USING PigStorage(';')
    AS (id:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- 2. Khía cạnh (aspect) có nhiều đánh giá tích cực (positive) nhất — giữ mọi aspect đồng hạng
-- 2a. Chỉ giữ các dòng có sentiment là positive
positive_data = FILTER data BY sentiment == 'positive';
-- 2b. Gom các review positive theo từng aspect (mỗi aspect một nhóm)
positive_aspect_group = GROUP positive_data BY aspect;
-- 2c. Với mỗi aspect: đếm số dòng positive → positive_cnt
positive_aspect_freq = FOREACH positive_aspect_group GENERATE group AS aspect, COUNT(positive_data) AS positive_cnt;
-- 2d. Gom toàn bộ bảng tần số vào một nhóm để tính max trên toàn bộ aspect
all_positive_aspect_freq = GROUP positive_aspect_freq ALL;
-- 2e. Một dòng duy nhất chứa giá trị max của positive_cnt (số positive cao nhất)
max_postive_cnt_aspect = FOREACH all_positive_aspect_freq GENERATE MAX(positive_aspect_freq.positive_cnt) as max_positive;
-- 2f. Ghép mỗi dòng (aspect, positive_cnt) với cột max_positive (cùng max cho mọi dòng)
postive_aspect_freq_with_max = CROSS positive_aspect_freq, max_postive_cnt_aspect;
-- 2g. Giữ các aspect có positive_cnt đúng bằng max → tất cả aspect đồng hạng nhất
top_positive_aspect = FILTER postive_aspect_freq_with_max BY positive_cnt == max_positive;
-- 2h. Định dạng một chuỗi hiển thị (một cột row_display) để lưu / in
top_positive_aspect_final = FOREACH top_positive_aspect GENERATE
    CONCAT('Aspect: ', aspect, ' | Number of positive comments: ', (chararray)positive_cnt) AS row_display;
-- 2i. Ghi kết quả positive ra HDFS (tab-separated, mỗi part một file part-r-*)
STORE top_positive_aspect_final INTO '/user/sonbui13/lab2/output/ex3_top_positive_aspect' USING PigStorage('\t');

-- 3. Khía cạnh có nhiều đánh giá tiêu cực (negative) nhất — cùng cách xử lý đồng hạng
-- 3a. Chỉ giữ các dòng có sentiment là negative
negative_data = FILTER data BY sentiment == 'negative';
-- 3b. Gom review negative theo aspect
negative_aspect_group = GROUP negative_data BY aspect;
-- 3c. Đếm số negative theo từng aspect
negative_aspect_freq = FOREACH negative_aspect_group GENERATE group AS aspect, COUNT(negative_data) AS negative_cnt;
-- 3d. Một nhóm chứa toàn bộ bảng để lấy max toàn cục
all_negative_aspect_freq = GROUP negative_aspect_freq ALL;
-- 3e. Giá trị negative_cnt lớn nhất trên mọi aspect
max_negative_cnt_aspect = FOREACH all_negative_aspect_freq GENERATE MAX(negative_aspect_freq.negative_cnt) as max_negative;
-- 3f. Mỗi dòng tần số kèm cột max_negative
negative_aspect_freq_with_max = CROSS negative_aspect_freq, max_negative_cnt_aspect;
-- 3g. Các aspect đạt max (đồng hạng)
top_negative_aspect = FILTER negative_aspect_freq_with_max BY negative_cnt == max_negative;
-- 3h. Chuỗi hiển thị cho nhánh negative
top_negative_aspect_final = FOREACH top_negative_aspect GENERATE
    CONCAT('Aspect: ', aspect, ' | Number of negative comments: ', (chararray)negative_cnt) AS row_display;
-- 3i. Ghi kết quả negative ra HDFS
STORE top_negative_aspect_final INTO '/user/sonbui13/lab2/output/ex3_top_negative_aspect' USING PigStorage('\t');

-- 4. In kết quả ra stdout khi chạy script (có thể lẫn với log Hadoop)
DUMP top_positive_aspect_final;
DUMP top_negative_aspect_final;