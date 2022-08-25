if (!require(pacman)) install.packages("pacman")
pacman::p_load("arrow", "read.dbc", "glue", "purrr", "dplyr", "stringr")


conversor_dbc <- function(files, origin_path, oformat) {
    if (oformat == "parquet") convert <- write_parquet
    if (oformat == "csv") convert <- write_csv_arrow
    walk(
        files,
        ~ read.dbc(glue("{origin_path}/{.x}")) %>%
            convert(sink = glue(
                "./Lista_3/{oformat}s/{str_replace(.x, 'dbc', oformat)}"
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

file.size("./Lista_3/parquets")
file.size("./Lista_3/csvs")

conversor_dbc(
    files = files[!GO_ES_MS],
    origin_path = path,
    oformat = "parquet"
)