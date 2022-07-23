pacman::p_load("vroom", "knitr", "dplyr", "glue", "tidyr")


# Questão 1
## Item b)
dados_path <- "./Lista_1/dados"
arquivo1 <- glue("{dados_path}/AC-Parte_1.csv")
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
arquivos <- glue("{dados_path}/{list.files(dados_path)}")

length(arquivos)
bytes <- file.size(arquivos) %>% sum()
bytes / 1024^2 # Megabytes
bytes / 1024^3 # Gigabytes

## Item d)
unique(primeiro$vacina_nome)
### ASTRAZENECA e ASTRAZENECA/FIOCRUZ

astrazeneca <- vroom(
    pipe(glue("grep -i ASTRAZENECA {arquivo1}")),
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

# object.size(primeiro)
# object.size(astrazeneca)

pacman::p_load("pryr", "microbenchmark")
object_size(astrazeneca) # [1] 17.55 kB
object_size(primeiro) # [1] 17.55 kB

microbenchmark(
    "pryr" = pryr::object_size(astrazeneca),
    "utils" = utils::object.size(astrazeneca),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Pacote", "Mínimo", "Média", "Máximo")) %>%
    kable(digits = 4)

RAM_primeiro <- object.size(primeiro) / 1024^2 #> 233,8 Megabytes
RAM_astrazeneca <- object.size(astrazeneca) / 1024^2 #> 80,1 Megabytes
RAM_primeiro - RAM_astrazeneca #> 153,7 Megabytes