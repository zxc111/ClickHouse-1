-- { echo }
select toDateTime64(toDateTime(1, 'Europe/Moscow'), 2);
select toDateTime64(toDate(1), 2, 'Europe/Moscow');
select toDateTime64(toDateTime(1, 'Europe/Moscow'), 2, 'GMT');
select toDateTime64(toDate(1), 2, 'GMT');
