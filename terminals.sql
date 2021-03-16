--Задача 1
--Составить sql запрос по задаче:
--Сделать выгрузку по терминалам, в которых в течение короткого периода времени (временного окна) провели оплаты несколько клиентов
--
--Входные данные:
--    • Период выгрузки с сентября 2019 по конец января 2020
--    • Временное окно операций - 2 часа
--    • Кол-во уникальных клиентов, которые совершили операции в окне - 3 и более клиентов
--
--Какие поля содержит рабочая таблица (operations):
--id - индекс (номер операции), тип int
--amount - сумма операции, тип float
--user_id - номер клиента, тип int
--term_id - номер банкомата, тип int
--created_at - дата операции, тип time stamp
--
--На выходе должна получиться таблица со списком операций удовлетворяющих условию.



with t1 as (
	select
		id,
		amount,
		user_id,
		term_id,
		created_at,
		created_at::date as date,
		case when created_at::hour-lag(created_at::hour)>2 then lag(hour2_flag)+1
			 else lag(hour2_flag) partition by term_id order by created_at asc as hour2_flag
	from operations

),
t2 as (
	select
		term_id,
		hour2_flag,
		count(distinct user_id) cnt
	from t1
	having cnt > 3
)
select * from t2