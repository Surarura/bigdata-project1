-- ========================================================================
-- KROK 1: Definicja tabel wejściowych (Metadane)
-- ========================================================================

-- Usuwamy stare tabele, jeśli istnieją, aby skrypt był powtarzalny
DROP TABLE IF EXISTS film_views_mr;
DROP TABLE IF EXISTS films;

-- Tabela dla wyniku MapReduce (datasource3)
-- Używamy ${hiveconf:input_dir3} - to zmienna, którą poda skrypt run_hive.sh
CREATE EXTERNAL TABLE film_views_mr (
    film_id STRING,
    platform STRING,
    total_views INT,
    avg_watch_time DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:input_dir3}';

-- Tabela dla drugiego zbioru danych (datasource4)
CREATE EXTERNAL TABLE films (
    film_id STRING,
    title STRING,
    director STRING,
    year_prod INT,
    genre STRING,
    duration_min INT,
    country STRING,
    lang STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:input_dir4}'
TBLPROPERTIES("skip.header.line.count"="1"); -- Ważne: pomijamy nagłówek!

-- ========================================================================
-- KROK 2 & 3: Przetwarzanie i zapis wyniku jako JSON
-- ========================================================================

-- Usuwamy starą tabelę wynikową
DROP TABLE IF EXISTS final_film_report;

-- Tworzymy tabelę wynikową, która będzie zapisana jako JSON
CREATE TABLE final_film_report
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '${hiveconf:output_dir6}';

-- Wstawiamy do niej dane za pomocą naszego zapytania
INSERT OVERWRITE TABLE final_film_report
WITH
    -- Krok 2.1: Połącz dane i oblicz wstępny stosunek dla każdej platformy
    film_platform_ratio AS (
        SELECT
            f.genre,
            f.title,
            f.film_id,
            v.total_views,
            -- Obliczamy stosunek: (średni czas oglądania / czas trwania filmu)
            -- Zabezpieczamy się przed dzieleniem przez 0
            CASE
                WHEN f.duration_min > 0 THEN (v.avg_watch_time / f.duration_min)
                ELSE 0
            END AS platform_ratio
        FROM
            film_views_mr v
        JOIN
            films f ON v.film_id = f.film_id
    ),

    -- Krok 2.2: Oblicz statystyki dla każdego FILMU
    film_stats AS (
        SELECT
            film_id,
            title,
            genre,
            -- Średni stosunek dla filmu (średnia ze wszystkich jego platform)
            AVG(platform_ratio) * 100 AS pct_film_watch_time,
            -- Łączna liczba odsłon na wszystkich platformach
            SUM(total_views) AS total_views_for_film
        FROM
            film_platform_ratio
        GROUP BY
            film_id, title, genre
    ),

    -- Krok 2.3: Oblicz statystyki dla każdego GATUNKU
    genre_stats AS (
        SELECT
            genre,
            -- Średni stosunek dla całego gatunku (średnia ze wszystkich platform wszystkich filmów w gatunku)
            AVG(platform_ratio) * 100 AS pct_genre_watch_time
        FROM
            film_platform_ratio
        GROUP BY
            genre
    )

-- Krok 2.4: Finalny SELECT i filtr
SELECT
    t_film.genre,
    t_film.title AS film_title,
    t_film.total_views_for_film AS total_views,
    t_film.pct_film_watch_time,
    t_genre.pct_genre_watch_time
FROM
    film_stats t_film
JOIN
    genre_stats t_genre ON t_film.genre = t_genre.genre
WHERE
    t_film.pct_film_watch_time > t_genre.pct_genre_watch_time;