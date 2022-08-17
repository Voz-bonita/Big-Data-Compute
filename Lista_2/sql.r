if (!require(pacman)) install.packages("pacman")
pacman::p_load("DBI", "RSQLite", "glue")
source("./Lista_2/banco.r")


# Questão 2
## Itens a) e b)
sql_conn <- dbConnect(RSQLite::SQLite(), "./Lista_2/lista2.sqlite")

dbWriteTable(sql_conn, "vax", vax_all)
dbWriteTable(sql_conn, "codigos", codigos)

seg_dose_query <- "SELECT * FROM vax
                   WHERE (vacina_descricao_dose='2ª Dose'
                   OR vacina_descricao_dose='2ª Dose Revacinação')"

fast_join <- glue("SELECT
                    (SELECT name_health_region
                        FROM codigos
                        WHERE (codigos.est_mun_codigo =
                            seg_dose.estabelecimento_municipio_codigo)
                            ) AS regiao_saude
                   FROM ({seg_dose_query}) AS seg_dose")

qnt_vax_query <- glue("SELECT regiao_saude, COUNT(*) AS N
                       FROM ({fast_join})
                       GROUP BY regiao_saude")

faixa_query <- glue("WITH qntVax AS ({qnt_vax_query})
                    SELECT regiao_saude, N,
                        CASE WHEN N > (SELECT MEDIAN(N) FROM qntVax)
                            THEN 'Alto'
                            ELSE 'Baixo'
                            END AS Faixa
                    FROM qntVax")

bot5_query <- glue("WITH tabFaixa AS ({faixa_query})
                    SELECT *
                    FROM (SELECT * FROM tabFaixa ORDER BY N ASC LIMIT 5)
                    UNION
                    SELECT *
                    FROM (SELECT * FROM tabFaixa ORDER BY N DESC LIMIT 5)
                    ORDER BY N")


ans <- dbGetQuery(sql_conn, bot5_query)

dbDisconnect(sql_conn)