if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "sparklyr", "dplyr", "glue", "purrr", "DBI",
    "geobr", "rjson", "stringr", "microbenchmark",
    "vroom"
)
source("./Lista_1/funcoes_aux.r")


codigos <- vroom(
    "./Lista_2_gabarito/Tabela_codigos.csv",
    col_types = list("Cód Região de Saúde" = col_character())
) %>%
    rename_all(~ c(
        "i", "abbrev_state", "municipio",
        "est_mun_codigo",
        "code_health_region", "name_health_region"
    ))


## Item d)
conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9
sc <- spark_connect(
    master = "local",
    version = "2.4.3",
    config = conf
)

tbl_cod <- copy_to(sc, codigos, name = "codigos", overwrite = TRUE)
tbl_vax <- spark_read_csv(sc = sc, path = "Lista_1/dados/*", delimiter = ";")


##################### Querys adapatadas para o Spark
prim_dose_query <- glue("SELECT estabelecimento_uf,
                                vacina_descricao_dose,
                                estabelecimento_municipio_codigo
                        FROM __THIS__
                        WHERE vacina_descricao_dose='1ª Dose'")

fast_join <- glue("SELECT name_health_region as regiao_saude
                   FROM ({prim_dose_query}) as prim_dose
                   LEFT JOIN codigos
                   ON prim_dose.estabelecimento_municipio_codigo
                       = codigos.est_mun_codigo")

qnt_vax_query <- glue("SELECT regiao_saude, COUNT(*) AS N
                       FROM ({fast_join})
                       GROUP BY regiao_saude")

faixa_query <- glue("WITH qntVax AS ({qnt_vax_query})
                    SELECT regiao_saude, N,
                        CASE WHEN N > (
                            SELECT percentile_approx(N, 0.5)
                            FROM qntVax)
                            THEN 'Alto'
                            ELSE 'Baixo'
                            END AS Faixa
                    FROM qntVax")

bot5_query <- glue("WITH tabFaixa AS ({faixa_query})
                    SELECT regiao_saude, N, Faixa FROM
                    (
                        SELECT *, dense_rank()
                        OVER (PARTITION BY Faixa ORDER BY N ASC) as posicao
                        FROM tabFaixa
                    )
                    WHERE posicao <= 5")


##################### Resultados em Memória
solve_spark <- function() {
    tbl_vax %>%
        ft_sql_transformer(bot5_query) %>%
        collect()
}

microbenchmark(
    "spark" = tbl_vax %>%
        ft_sql_transformer(bot5_query) %>%
        collect(),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Solução", "Mínimo", "Média", "Máximo")) %>%
    format_tab(caption = "Teste", digits = 1L)

spark_disconnect(sc)