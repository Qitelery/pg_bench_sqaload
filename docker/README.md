	  ### Подготовка
0. Если используете Windows и вы устанавливали клиент PostgreSQL, то проверьте в службах Windows, что данная служба отключена.

1. Запускаем Docker Desktop

2. Из папки ./pg-bench-sqa/docker для развертывания бд и сопутствующих сервисов запускаем:
docker-compose up -d

3. Подключаемся к контейнеру с БД Postgre:
docker exec -it postgres bash

4. Инициализируем таблицы pg_bench
pgbench -i -U postgres postgres

5. Запускаем наш первый тест (в тесте будут использоваться синтетические данные, инициализированных ранее таблиц) 
pgbench -U postgres -c 10 -j 2 -T 60 postgres
Здесь:
 -U postgres указывает pgbench использовать пользователя postgres для подключения к базе данных.
 -c 10 говорит о том, что должно быть 10 клиентских соединений.
 -j 2 задает количество потоков (threads).
 -T 60 устанавливает продолжительность теста в 60 секунд.
 postgres — это имя базы данных, которую вы хотите тестировать
 
 
	  ###Чтение результата теста
Пример:
transaction type: ./pgb.sql  - имя сценария
scaling factor: 1            - фактор масштабирования
query mode: simple	         - режим запросов
number of clients: 20		 - количество клиентов
number of threads: 4		 - количество потоков от каждого клиента
duration: 60 s	        	 - длительность теста в секундах
number of transactions actually processed: 4436	- фактическое количество обработанных транзакций
latency average = 271.534 ms	-  среднее время задержки при выполнении запросов в тесте.
tps = 73.655667 (including connections establishing)	- TPS, включая установление соединений с базой данных
tps = 73.659515 (excluding connections establishing)	- TPS, исключая установление соединений с базой данных
 
	С точки зрения оценки проиозводительности, при заданных условиях, используются: number of transactions actually processed, latency average, tps (количество транзакций в секунду).
 
 
      ###Пользовательский кейс №1
В каталоге ./pg-bench-sqa/docker/postgres/pgbench-results в файле pgb.sql лежит пользовательский сценарий теста

1. Дополнительно генерируем данные 
DO $$
BEGIN
    FOR i IN 5433..1000000 LOOP
        INSERT INTO purchases (id, amount, order_id, product_id)
        VALUES (i,
                TRUNC(RANDOM() * 3 + 1), -- случайное значение для amount от 1 до 4
                TRUNC(RANDOM() * 993 + 1), -- случайное значение для order_id от 1 до 994
                TRUNC(RANDOM() * 998 + 1002)); -- случайное значение для product_id от 1002 до 2000
    END LOOP;
    COMMIT;
END;
$$;

2. 

a) Смотрим реальный план запроса: EXPLAIN analyze SELECT product_id FROM purchases WHERE product_id < 1520;

Замеряем время выполнения запроса.

b) Редактируем сценарий к виду:
\SET product random(1002,2000) 
BEGIN;
SELECT product_id FROM purchases WHERE product_id < :product;
END;

c) 
В консоли переходим в папку pgbench-results:  cd pgbench-results
Запускаем тест: pgbench -U postgres -c 20 -j 4 -T 60 -f ./pgb.sql postgres > "название файла с результатами теста".txt

3. 
a) Создаем индекс
CREATE INDEX pgb_product ON purchases (product_id);

b) Смотрим реальный план запроса:
EXPLAIN analyze SELECT product_id FROM purchases WHERE product_id < 1520;

Замеряем время выполнения и обращаем внимание, что используется SeqScan.
При сравнении с временем выполнения увидим, что оно меньше, чем в п.1.а.

c)Запускаем тест: pgbench -U postgres -c 20 -j 4 -T 60 -f ./pgb.sql postgres > "название файла с результатами теста".txt

4.
a) Выполняем следующие запросы:
SET enable_bitmapscan = off;

SET enable_indexscan = on;

SET enable_seqscan = off;

SET max_parallel_workers_per_gather = 0;

b) Смотрим реальный план запроса:
EXPLAIN analyze SELECT product_id FROM purchases WHERE product_id < 1520;

Замеряем время выполнения и обращаем внимание, что используется IndexOnlyScan.
При сравнении с временем выполнения увидим, что оно меньше, чем в п.1.а и п.2.b.

с)Редактируем сценарий к виду:
\SET product random(1002,2000) 
BEGIN;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SET max_parallel_workers_per_gather = 0;
SELECT product_id FROM purchases WHERE product_id < :product;
END;

d) Запускаем тест: pgbench -U postgres -c 20 -j 4 -T 60 -f ./pgb.sql postgres > "название файла с результатами теста".txt

5. Предлагается посмотреть на результаты двух пользовательских тестов (п.2 и п.3), и сопоставить метрики производительности с разницей во
времени выполнения запросов, замеренной с помощью EXPLAIN ANALYZE.