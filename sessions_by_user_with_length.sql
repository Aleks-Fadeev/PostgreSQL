SELECT DISTINCT user_id, start_time, end_time, session_length_in_minute
-- выбираем только строки, где посещение страниц в нужном нам порядке "WHERE pages like '%1%2%3%'"
FROM (
-- Определяем параметры сессии: начало, конец, длительность, при помощи функции string_agg() создаем строку, в которой 
-- будет находиться последовательность из всех страниц, которые посетил пользователь
	Select 
		user_id, 
		first_value as start_time, 
		last_value + interval '1 hour' as end_time, 
		EXTRACT(EPOCH FROM last_value + interval '1 hour' - first_value)/60 as session_length_in_minute,
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