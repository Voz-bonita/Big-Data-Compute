pacman::p_load("vroom", "knitr", "dplyr")


# Questão 1
## Item b)
dados_path <- "./Lista_1/dados/"
arquivo1 <- paste0(dados_path, "AC-Parte_1.csv")
primeiro <- vroom(arquivo1,
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
arquivos <- paste0(
    dados_path,
    list.files(dados_path)
)

length(arquivos)
bytes <- file.size(arquivos) %>% sum()
bytes / 1024^2 # Megabytes
bytes / 1024^3 # Gigabytes

## Item d)
unique(primeiro$vacina_nome)
### ASTRAZENECA e ASTRAZENECA/FIOCRUZ

astrazeneca <- vroom(
    pipe(paste0("grep -i ASTRAZENECA ", arquivo1)),
    delim = ";",
    num_threads = 4,
    locale = locale(
        grouping_mark = ".",
        decimal_mark = ",",
        encoding = "UTF-8"
    ),
    show_col_types = FALSE,
    col_names = names(primeiro)
)