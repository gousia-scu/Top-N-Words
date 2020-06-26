set hive.exec.compress.output=true;
set mapred.compress.map.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set mapreduce.input.fileinputformat.split.maxsize=160000000;
set mapreduce.job.reduces=200;


CREATE DATABASE final;
use final;
CREATE TABLE hive (line STRING);
LOAD DATA INPATH 'BigData.txt' OVERWRITE INTO TABLE hive;
CREATE TABLE word_count AS
SELECT word, count(1) AS count 
FROM(SELECT explode(split(REGEXP_REPLACE(line,'[\\p{Punct}]',' '),' ')) AS word FROM hive)words ----The regexp_replace here replaces the punctuaion in the column line to spaces.
GROUP BY word ---- selects and displays results of all the words formed
ORDER BY count DESC, word ASC; ----The count desc displays words in descending order
select * from word_count where LENGTH(word)>6 limit 100; ----selects top 100 most frequent words with length greater than 6
select * from word_count limit 100;  ----selects and displays top 100 words from the table word_count

use final;
drop table hive;
drop table word_count;
drop database final CASCADE;