SELECT toTimeZone(EventTime, 'Europe/Moscow') as EventTime FROM remote('127.0.0.{1,2}', test, hits) ORDER BY EventTime DESC LIMIT 10
