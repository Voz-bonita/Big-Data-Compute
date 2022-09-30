if (!require(pacman)) install.packages("pacman")
pacman::p_load("DBI", "RSQLite", "glue", "microbenchmark")
source("./Lista_2_gabarito/banco.r", encoding = "utf-8")
source("./Lista_1/funcoes_aux.r")


# Questão 2
## Itens a) e b)
sql_conn <- dbConnect(RSQLite::SQLite(), "./Lista_2_gabarito/lista2.sqlite")

dbWriteTable(sql_conn, "vax", vax_all)
dbWriteTable(sql_conn, "codigos", codigos)

prim_dose_query <- "SELECT * FROM vax
                   WHERE vacina_descricao_dose='1ª Dose'"

fast_join <- glue("SELECT
                    (SELECT name_health_region
                        FROM codigos
                        WHERE (codigos.est_mun_codigo =
                            prim_dose.estabelecimento_municipio_codigo)
                            ) AS regiao_saude
                   FROM ({prim_dose_query}) AS prim_dose")

qnt_vax_query <- glue("SELECT regiao_saude, COUNT(*) AS N
                       FROM ({fast_join})
                       GROUP BY regiao_saude")

faixa_query <- glue("WITH qnt_vax AS ({qnt_vax_query})
                    SELECT regiao_saude, N,
                        CASE WHEN N > (SELECT MEDIAN(N) FROM qnt_vax)
                            THEN 'Alto'
                            ELSE 'Baixo'
                            END AS Faixa
                    FROM qnt_vax")

bot5_query <- glue("WITH tabFaixa AS ({faixa_query})
                    SELECT *
                    FROM (
                        SELECT *
                        FROM tabFaixa
                        WHERE Faixa='Baixo'
                        ORDER BY N ASC LIMIT 5)
                    UNION
                    SELECT *
                    FROM (
                        SELECT *
                        FROM tabFaixa
                        WHERE Faixa='Alto'
                        ORDER BY N ASC LIMIT 5)
                    ORDER BY N")

solve_sql <- function() {
    DBI::dbGetQuery(sql_conn, bot5_query)
}

microbenchmark(
    "SQLite" = solve_sql(),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Solução", "Mínimo", "Média", "Máximo")) %>%
    format_tab(caption = "", digits = 1L)

dbDisconnect(sql_conn)