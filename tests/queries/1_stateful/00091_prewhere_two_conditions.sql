SET max_bytes_to_read = 600000000;

SET optimize_move_to_prewhere = 1;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND EventTime < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow');
SELECT uniq(URL) FROM test.hits WHERE EventTime >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND URL != '' AND EventTime < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow');
SELECT uniq(*) FROM test.hits WHERE EventTime >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND EventTime < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow') AND EventDate = '2014-03-21';
WITH EventTime AS xyz SELECT uniq(*) FROM test.hits WHERE xyz >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND xyz < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow') AND EventDate = '2014-03-21';

SET optimize_move_to_prewhere = 0;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND EventTime < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow'); -- { serverError 307 }
SELECT uniq(URL) FROM test.hits WHERE EventTime >= toDateTime('2014-03-20 00:00:00', 'Europe/Moscow') AND URL != '' AND EventTime < toDateTime('2014-03-21 00:00:00', 'Europe/Moscow'); -- { serverError 307 }
