--Хотел просто изменить таблицы, но сильно запутался.
--Решил удалить все (DROP SCHEMA...CASCADE) 
--и сделать все заново с момента создания схемы car_shop.
 

CREATE SCHEMA raw_data;
--создаю схему для сырых данных

CREATE TABLE raw_data.sales ( 
	id int PRIMARY KEY,
	auto text,
	gazoline_consumption decimal(3, 1),
	price decimal (7,
2),
	date timestamp,
	person text,
	phone text,
	discount decimal (3,
1),
	brand_origin text
);

/*В терминале macos
 * PGPASSWORD='ba64a00ffc8947deb7b82bca28d956eb' PGSSLMODE=require \
psql -h c-c9qe9p3arimg4hftnfrh.rw.mdb.yandexcloud.net -p 6432 \
     -U de_start_20250705_0218fc2d2c -d playground_start_20250705_0218fc2d2c 

psql (17.5, server 15.14 (Ubuntu 15.14-201-yandex.55518.14158a575f))
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off, ALPN: none)
Type "help" for help.

Скопировал сырые данные
playground_start_20250705_0218fc2d2c=> \copy raw_data.sales FROM '/Users/niazbekmamisov/Desktop/DE/YandexPracticum/cars.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'null');
COPY 1000 */


SELECT
	count(*)
FROM
	raw_data.sales;

SELECT
	*
FROM
	raw_data.sales
LIMIT 10;

CREATE SCHEMA car_shop;

CREATE TABLE car_shop.purchaser (
	purchaser_id SERIAL PRIMARY KEY,
	name text NOT NULL,
	phone text
);

INSERT
	INTO
	car_shop.purchaser (name,
	phone)
SELECT
	DISTINCT person,
	phone
FROM
	raw_data.sales
ORDER BY
	person;

CREATE TABLE car_shop.brand_origin (
	brand_origin_id SERIAL PRIMARY KEY,
	country varchar NOT NULL 
);

INSERT
	INTO
	car_shop.brand_origin (country)
SELECT
	DISTINCT brand_origin
FROM
	raw_data.sales s
WHERE
	s.brand_origin IS NOT NULL
ORDER BY
	brand_origin;

CREATE TABLE car_shop.color (
	color_id SERIAL PRIMARY KEY,
	color_name varchar NOT NULL
);

INSERT
	INTO
	car_shop.color (color_name)
SELECT
	DISTINCT TRIM(split_part(s.auto, ',', 2))
FROM
	raw_data.sales s
ORDER BY
	TRIM(split_part(s.auto, ',', 2));

SELECT
	auto,
	brand_origin
FROM
	raw_data.sales
WHERE
	brand_origin IS NULL;

UPDATE
	raw_data.sales
SET
	brand_origin = 'Germany'
WHERE
	brand_origin IS NULL
	AND auto ILIKE '%Porsche%';

CREATE TABLE car_shop.brand (
	brand_id SERIAL PRIMARY KEY,
	brand_name varchar NOT NULL,
	brand_origin_id int REFERENCES car_shop.brand_origin(brand_origin_id)
);

INSERT
	INTO
	car_shop.brand (brand_name,
	brand_origin_id)
SELECT
	DISTINCT trim(split_part(split_part(s.auto, ',', 1), ' ', 1)) AS brand_name,
	bo.brand_origin_id
FROM
	raw_data.sales s
LEFT JOIN car_shop.brand_origin bo ON
	bo.country = s.brand_origin
ORDER BY
	trim(split_part(split_part(s.auto, ',', 1), ' ', 1));

CREATE TABLE car_shop.model (
	model_id serial PRIMARY KEY,
	model_name varchar NOT NULL,
	brand_id int REFERENCES car_shop.brand(brand_id),
	gazoline_consumption decimal(3, 1)
);

INSERT INTO car_shop.model (model_name, brand_id, gazoline_consumption)
SELECT DISTINCT
	 TRIM(substring(split_part(auto, ',', 1) from position(' ' in split_part(auto, ',', 1)) + 1)) AS model_name,
	 b.brand_id,
	 s.gazoline_consumption
FROM raw_data.sales s
JOIN car_shop.brand b 
	ON b.brand_name = trim(split_part(split_part(s.auto, ',', 1), ' ', 1));

CREATE TABLE car_shop.sales (
	sales_id SERIAL PRIMARY KEY,
	model_id int REFERENCES car_shop.model(model_id),
	color_id int REFERENCES car_shop.color(color_id),
	price decimal(7,2),                  -- цена С УЧЁТОМ скидки 
	sales_date timestamp,
	purchaser_id int REFERENCES car_shop.purchaser(purchaser_id),
	discount decimal(3,1)                -- % скидки
);

INSERT INTO car_shop.sales (model_id, color_id, price, sales_date, purchaser_id, discount)
SELECT 
	m.model_id,
	c.color_id,
	s.price,
	s.date,
	p.purchaser_id,
	s.discount
FROM raw_data.sales s 
JOIN car_shop.model m ON m.model_name = TRIM(substring(split_part(auto, ',', 1) from position(' ' in split_part(auto, ',', 1)) + 1))
JOIN car_shop.color c ON c.color_name = TRIM(split_part(s.auto, ',', 2))
JOIN car_shop.purchaser p ON p.name = s.person;


-- Этап 2. Создание выборок 

 

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.

SELECT round(COUNT(*) FILTER (WHERE m.gazoline_consumption IS NULL)::NUMERIC / COUNT(*) * 100, 2) AS nulls_percentage_gasoline_consumption
FROM car_shop.model m;

---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.

SELECT 
	b.brand_name,
	EXTRACT(YEAR FROM s.sales_date) AS year,
	ROUND(AVG(s.price),2) AS price_avg
FROM car_shop.sales s
JOIN car_shop.model m ON s.model_id = m.model_id
JOIN car_shop.brand b ON m.brand_id = b.brand_id
GROUP BY b.brand_name, EXTRACT(YEAR FROM s.sales_date)
ORDER BY b.brand_name, EXTRACT(YEAR FROM s.sales_date);

---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки. 

SELECT 
	EXTRACT(MONTH FROM s.sales_date) AS month,
	EXTRACT(YEAR FROM s.sales_date) AS year,
	ROUND(AVG(s.price),2) AS price_avg
FROM car_shop.sales s
WHERE EXTRACT(YEAR FROM s.sales_date) = 2022
GROUP BY 
	EXTRACT(MONTH FROM s.sales_date),
	EXTRACT(YEAR FROM s.sales_date)
ORDER BY 1;

---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.

SELECT 
	p.name AS person,
	string_agg(concat(b.brand_name, ' ', m.model_name), ', ') AS cars
FROM car_shop.purchaser p 
JOIN car_shop.sales s ON p.purchaser_id = s.purchaser_id
JOIN car_shop.model m ON m.model_id = s.model_id
JOIN car_shop.brand b ON b.brand_id = m.brand_id
GROUP BY p.name 
ORDER BY p.name;

---- Задание 5 Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране без учёта скидки.

SELECT bo.country,
	MAX(s.price / 1 - s.discount/100) AS price_max,
	MIN(s.price / 1 - s.discount/100) AS price_min
FROM
	car_shop.brand_origin bo 
JOIN car_shop.brand b ON
	bo.brand_origin_id = b.brand_origin_id
JOIN car_shop.model m ON
	b.brand_id = m.brand_id
JOIN car_shop.sales s ON 
	m.model_id = s.model_id
GROUP BY
	bo.country;

---- Задание 6. Напишите запрос, который покажет количество всех пользователей из США.

SELECT
	COUNT(*) AS person_from_usa_count
FROM
	car_shop.purchaser p
WHERE
	p.phone LIKE '+1%';
