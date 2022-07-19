pacman::p_load("vroom", "knitr", "dplyr")


# Questão 1
## Item b)
dados_path <- "./Lista_1/dados/"
primeiro <- vroom(paste0(dados_path, "AC-Parte_1.csv"),
    delim = ";",
    num_threads = 4,
    locale = locale(
        grouping_mark = ".",
        decimal_mark = ",",
        encoding = "UTF-8"
    ),
    show_col_types = FALSE
)
head(primeiro[3:6L]) %>%
    kable()

spec(primeiro)

## Item c)
pacman::p_load("purrr")
arquivos <- paste0(
    dados_path,
    list.files(dados_path)
)
length(arquivos)
map_dbl(arquivos, ~ file.info(.x)[["size"]]) %>%
    sum() / 10^6