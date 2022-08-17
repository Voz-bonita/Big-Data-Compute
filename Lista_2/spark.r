if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "sparklyr", "dplyr", "glue", "purrr", "DBI",
    "geobr", "rjson", "stringr", "microbenchmark"
)
source("./Lista_1/funcoes_aux.r")

dados_path <- ".\\Lista_1\\dados"
arquivos <- glue("{dados_path}\\{list.files(dados_path)}")

reg_saude <- read_health_region()
ibge_geobr <- fromJSON(file = "./Lista_1/mapa_codigos.json") %>% unlist()
codigos <- data.frame(
    "est_mun_codigo" = names(ibge_geobr),
    "code_health_region" = ibge_geobr
) %>%
    merge(reg_saude, by = "code_health_region") %>%
    select(est_mun_codigo, name_health_region)


## Item d)
sc <- spark_connect(master = "local", version = "3.0.0")

tbl_cod <- copy_to(sc, codigos, name = "codigos", overwrite = TRUE)
tbl_vax <- map(
    arquivos,
    ~ spark_read_csv(
        sc = sc, path = .x,
        delimiter = ";"
    )
) %>%
    reduce(dplyr::union_all)


##################### Querys adapatadas para o Spark
seg_dose_query <- glue("SELECT * FROM __THIS__
                   WHERE (vacina_descricao_dose='2ª Dose'
                   OR vacina_descricao_dose='2ª Dose Revacinação')")

fast_join <- glue("SELECT name_health_region as regiao_saude
                   FROM ({seg_dose_query}) as seg_dose
                   LEFT JOIN codigos
                   ON seg_dose.estabelecimento_municipio_codigo
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
                        OVER (PARTITION BY Faixa ORDER BY N DESC) as posicao
                        FROM tabFaixa
                    )
                    WHERE posicao <= 5")


##################### Resultados em Memória
tbl_vax %>%
    ft_sql_transformer(bot5_query) %>%
    collect()

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