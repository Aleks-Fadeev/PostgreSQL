WITH one as (
SELECT DISTINCT user_id, start_time, end_time, session_length_minute
-- выбираем только строки, где посещение страниц в нужном нам порядке "WHERE pages like '%1%2%3%'"
FROM (
-- Определяем параметры сессии: начало, конец, длительность, при помощи функции string_agg() создаем строку, в которой 
-- будет находиться последовательность из всех страниц, которые посетил пользователь
	Select 
		user_id, 
		first_value as start_time, 
		last_value + interval '1 hour' as end_time, 
		EXTRACT(EPOCH FROM last_value + interval '1 hour' - first_value)/60 as session_length_minute,
		STRING_AGG (CAST(page as text), ', ') OVER (PARTITION by user_id,label) as pages
	From(
-- Находим начало и конец каждой сессии каждого пользователя при помощи first_value и last_value,
		select 
			user_id, 
			activity_rank, 
			page, 
			label, 
			last_value(happened_at) over (partition by user_id, label 
				order by activity_rank rows between unbounded preceding and unbounded following),
			first_value(happened_at) over (partition by user_id, label 
				order by activity_rank rows between unbounded preceding and unbounded following)
		from(
-- 3-й подзапрос: создаем поле label, в котором суммируем при помощи оконной функции поле isnewgroup – получаем столбец, 
-- в котором с началом каждой новой сессии значение прирастает на 1.
			select 
				user_id, 
				activity_rank, 
				page, 
				happened_at,  
				sum(isnewgroup) over (order by user_id, activity_rank
				rows between unbounded preceding and current row) as label
			from(
--2-й подзапрос: определяем, какое из действий каждого пользователя открывает новую сессию (более часа с предыдущего действия) -
-- поле isnewgroup
				select 
					user_id, 
					activity_rank, 
					page, 
					happened_at,
					lag(happened_at) over (partition by user_id order by user_id, activity_rank),
					(case when (happened_at - lag(happened_at) over (partition by user_id order by user_id, activity_rank
					) > '01:00:00.000') then 1 
					when lag(happened_at) over (partition by user_id order by user_id, activity_rank) IS NULL then 1 
					else 0 end) as isnewgroup
					From(
-- 1-й подзапрос: при помощи CASE маркируем номер страницы из нашего целевого списка – 1,2,3 или 0 - для остальных. Определяем 
-- номер страницы в порядке посещения для каждого пользователя – activity_rank.
						select 
						user_id, 
						happened_at, 
						(case page when 'page1' then 1 
						when 'page2' then 2 
						when 'page3' then 3 else 0 end) as page,
						rank() over (partition by user_id order by happened_at) as activity_rank
						from time_interval) as q
				) as q1
			) as q2
	order by user_id, activity_rank, label
		) as q3
	)as q4
WHERE pages like '%1%2%3%'
ORDER BY start_time
),
-- результатом CTE "one" будет набор строк с user_id, временем начал и конца сессии и длительностью сессии в минутах.

two AS (SELECT t AS time1,
	lead(t) over(ORDER BY t) AS time2
	FROM (
		SELECT start_time AS t FROM one
		UNION ALL
		SELECT end_time AS t FROM one
		) as u
ORDER BY t),
-- результатом CTE "two" будет таблица, где в строках перебираются все временные интервалы между действиями (начало сессии,
-- конец сессии). Действия располагаются в хронологическом порядке.

three as (SELECT to_char(two.time1, 'hh24:mi:ss') AS time1,
to_char(two.time2, 'hh24:mi:ss') AS time2,
(SELECT COUNT(*) FROM one WHERE one.start_time<=two.time1 AND two.time2<=one.end_time) AS cnt
FROM two
WHERE two.time2 IS NOT NULL)
-- результатом CTE "three" являются временные интервалы между действиями и 
-- количество одновременно открытых сессий в этом интервале

SELECT time1, time2, cnt
FROM three
-- добавляем условие вывода только временных интервалов с максимальным открытым количеством сессий
WHERE cnt = (SELECT MAX(cnt) 
			 FROM three)