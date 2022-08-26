if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "arrow", "read.dbc", "glue",
    "purrr", "dplyr", "stringr",
    "microbenchmark", "sparklyr",
    "furrr"
)


## Item b)
conversor_dbc <- function(files, origin_path, oformat) {
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
file.size(parquets) %>% sum() / 1024^2 %>% round(2)
file.size(csvs) %>% sum() / 1024^2 %>% round(2)



## Item c)
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