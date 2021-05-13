SELECT toTimeZone(EventTime, 'Europe/Moscow') as EventTime FROM test.hits ORDER BY EventTime DESC LIMIT 10
