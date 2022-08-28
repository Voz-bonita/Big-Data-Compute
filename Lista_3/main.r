if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "arrow", "read.dbc", "glue",
    "furrr", "dplyr", "stringr",
)


## Item b)
conversor_dbc <- function(files, origin_path, oformat) {
    "Recebe os arquivos dbc, seu caminho de origem e o formato de saída
    e abre um plano de multisessões do future para fazer a conversão"
    future::plan(future::multisession, workers = 7)
    if (oformat == "parquet") convert <- write_parquet
    if (oformat == "csv") convert <- write_csv_arrow

    # Gambiarra do future
    origin_path <- origin_path

    future_walk(
        files,
        ~ read.dbc(glue("{origin_path}/{.x}")) %>%
            convert(sink = glue(
                "./Lista_3/{oformat}s/{
                    stringr::str_replace(.x, 'dbc', oformat)
                }"
            ))
    )
}

path <- "./Lista_3/datasus"
files <- list.files(path)
GO_ES_MS <- str_detect(files, "GO|MS|ES")

conversor_dbc(
    files = files[GO_ES_MS],
    origin_path = path,
    oformat = "parquet"
)
conversor_dbc(
    files = files[GO_ES_MS],
    origin_path = path,
    oformat = "csv"
)

parquets <- list.files("./Lista_3/parquets/", full.names = TRUE)
csvs <- list.files("./Lista_3/csvs/", full.names = TRUE)
file.size(parquets) %>% sum() / 1024^2
file.size(csvs) %>% sum() / 1024^2


## Item c)
pacman::p_load("microbenchmark", "sparklyr")
source("./Lista_1/funcoes_aux.r")

sc <- spark_connect(master = "local", version = "2.4.3")

microbenchmark(
    "parquet" = spark_read_parquet(sc, path = "Lista_3/parquets/*"),
    "csv" = spark_read_csv(sc, path = "Lista_3/csvs/*"),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Solução", "Mínimo", "Média", "Máximo")) %>%
    format_tab(
        caption = "Comparação entre o tempo coputacional, em segundos,
        para ler os aquivos .parquet e .csv do SINASC, nos estados ES, GO e MS",
        digits = 1L
    )

conversor_dbc(
    files = files[!GO_ES_MS],
    origin_path = path,
    oformat = "parquet"
)


# Questão 2

sinasc <- spark_read_parquet(
    sc,
    path = "Lista_3/teste/*",
    infer_schema = FALSE
) %>%
    select(-c(
        contador, LOCNASC, CODMUNNASC,
        CODMUNRES, DTNASC, APGAR1, APGAR5,
        RACACOR, CODANOMAL
    ))

sinasc %>%
    select(IDADEMAE, QTDFILVIVO, QTDFILMORT) %>%
    sdf_describe() %>%
    mutate_at(c("IDADEMAE", "QTDFILVIVO", "QTDFILMORT"), as.numeric) %>%
    rename_all(
        ~ c(
            "Medida", "Idade da Mãe",
            "Nº de Filhos vivos", "Nº de Filhos mortos"
        )
    ) %>%
    format_tab(
        cap = "Medidas descritivas das variáveis quantitivas do SINASC",
        digits = 2,
        "latex"
    )