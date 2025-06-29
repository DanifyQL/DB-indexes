-- Запрос определяет количество неоплаченных заказов 

/*
EXPLAIN ANALYZE
SELECT
	count(*)
FROM
	order_statuses os
JOIN
	orders o ON o.order_id = os.order_id
WHERE
	(SELECT count(*) FROM order_statuses os1 WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0 AND
	o.city_id = 1;

actual time = 18479.016
cost = 61241239.48

*/

-- ОПТИМИЗАЦИЯ
-- создаем индекса для быстрого объединения таблиц
CREATE INDEX order_statuses_order_id_idx ON order_statuses(order_id); 
-- создаем индекс для быстрого отбора order_id по status_id
CREATE INDEX order_statuses_status_id__idx ON order_statuses(status_id); 
-- создаем индекс для ускорения фильтрации
CREATE INDEX orders_city_id_idx ON orders(city_id); 

-- EXPLAIN ANALYZE
SELECT
	COUNT(order_id)
FROM
	order_statuses os
JOIN
	orders o ON os.order_id = o.order_id
WHERE NOT EXISTS
	(SELECT 1 FROM order_statuses os1 WHERE os1.order_id = o.order_id AND os1.status_id = 2) AND
	o.city_id = 1;

/* actual time = 6.327
 cost = 2592.13
 ускорение в 2920 раз
Ключ оптимизации - полностью избавились от всех Seq Scan и излишних Aggregate.
*/

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Запрос ищет логи за текущий день
/*
EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

actual time = 399.823
cost = 155991.55
*/

-- ОПТИМИЗАЦИЯ
--EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE datetime >= CURRENT_DATE -- лучше использовать условие, чем точное совпадение. PostgreSQL не колоночная субд, а строковая.
							    -- Также у нас уже есть индекс по дате. Убираем лишний каст с поля datetime к типу date.
-- actual time = 0.014 
-- cost = 37.38
-- ускорение в 28559 раз

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Запрос ищет действия и время действия определенного посетителя

/*
EXPLAIN ANALYZE
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

actual time = 139.652
cost = 92137.50
*/

-- ОПТИМИЗАЦИЯ
-- Запрос остается без изменений. Создаем индексы на основую таблицу и на партиции. Можно создать только на партиции. 
-- Индекс создаем на каждую партицию т.к. на родительской таблице он создается не по первичному ключу и не применяется к партицированным таблицам автоматически.
CREATE INDEX user_logs_visitor_control_idx ON user_logs(visitor_uuid)
INCLUDE (event, datetime);

CREATE INDEX user_logs_y2021q2_visitor_control_idx ON user_logs_y2021q2(visitor_uuid)
INCLUDE (event, datetime);

CREATE INDEX user_logs_y2021q3_visitor_control_idx ON user_logs_y2021q3(visitor_uuid)
INCLUDE (event, datetime);

CREATE INDEX user_logs_y2021q4_visitor_control_idx ON user_logs_y2021q4(visitor_uuid)
INCLUDE (event, datetime);

-- actual time = 0.512 
-- cost = 53.01
-- ускорение в 273 раза

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Запрос вычисляет количество заказов позиций, продажи которых выше среднего по убыванию
/*
EXPLAIN ANALYZE
SELECT d.name,
	   SUM(count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
				   SELECT item
				   FROM (
				   		  SELECT
						  	item,
							SUM(count) AS total_sales
		  				  FROM
								order_items oi
		  				  GROUP BY 1
						 ) AS dishes_sales
					WHERE dishes_sales.total_sales > (
														SELECT SUM(t.total_sales) / COUNT(*)
														FROM (
															   SELECT item,
															   SUM(count) AS total_sales
															   FROM order_items oi
																GROUP BY 1
															  ) t
													   )
				)
GROUP BY 1
ORDER BY orders_quantity DESC;

actual time = 42.536
cost = 4810.74
*/

-- ОПТИМИЗАЦИЯ
-- Воспользуемся CTE для облегчения расчета и вычисления его всего один раз до основного запроса. Потом применим для фильтрации.
-- Также добавим индексы на ключевые поля запроса

CREATE INDEX order_items_quantity_idx ON order_items(item, count);
CREATE INDEX order_items_item_idx ON order_items(item);

EXPLAIN ANALYZE
WITH 
total_sales AS (
SELECT
	item,
	SUM(count) total_sales
FROM
	order_items
GROUP BY 1
),

avg_sales AS (
SELECT
	AVG(total_sales) avg_sales 
FROM
	total_sales
),

above_avg_sales AS (
SELECT
	item
FROM
	total_sales
WHERE
	total_sales > (SELECT avg_sales FROM avg_sales)
)

SELECT
	d.name,
	SUM(count) orders_quantity
FROM
	order_items oi
JOIN
	dishes d ON d.object_id = oi.item
WHERE
	oi.item IN (SELECT item FROM above_avg_sales)
GROUP BY 1
ORDER BY
	orders_quantity DESC;

-- actual time = 23.727
-- cost = 2514.73
-- ускорение в 1.8 раз

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	
-- Запрос выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
/*
EXPLAIN ANALYZE
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
JOIN orders o ON o.order_id = os.order_id
JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
						  SELECT max(status_dt)
						  FROM order_statuses
						  WHERE order_id = o.order_id
    					 );
						 
actual time = 0.065
cost = 78.34
*/

-- ОПТИМИЗАЦИЯ
CREATE INDEX orders_users_id_idx ON orders(user_id)
INCLUDE (order_id, order_dt, final_cost);

CREATE INDEX statuses_idx ON statuses(status_id, status_name);

EXPLAIN ANALYZE
SELECT
	q1.order_id,
	q1.order_dt,
	q1.final_cost,
	q1.status_name
FROM
(
	SELECT
		q.order_id AS order_id,
	  	q.order_dt AS order_dt,
		q.final_cost AS final_cost,
	  	os.status_dt AS status_dt,
	   	s.status_name AS status_name,
	   	MAX(os.status_dt) OVER (PARTITION BY q.order_id) AS last_status
	FROM
	(
		SELECT
			o.order_id AS order_id,
			o.order_dt AS order_dt,
			o.final_cost AS final_cost
	  	FROM
			orders o
	  	WHERE
			o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	 ) AS q
	JOIN
		order_statuses os ON q.order_id = os.order_id
	JOIN
		statuses s ON os.status_id = s.status_id
) AS q1
WHERE
	q1.status_dt = q1.last_status;

-- actual time = 0.046
-- cost = 37.27
-- ускорение в 1.4 раза. Использование смециального узла Memoize.
/*
Оптимизация за счет работы с малым количеством строк, т.к. мы сразу оставляем только заказы необходимого пользователя. 
После чего уже присоединяем все остальные данные и фильтруем их.
*/