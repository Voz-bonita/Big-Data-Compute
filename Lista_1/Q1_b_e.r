pacman::p_load("vroom", "knitr", "dplyr", "glue", "tidyr")


# Questão 1
## Item b)
dados_path <- "./Lista_1/dados"
arquivo1 <- glue("{dados_path}/AC-Parte_1.csv")
primeiro <- vroom(arquivo1,
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE
)
head(primeiro[3:6L]) %>%
    kable()

spec(primeiro)

## Item c)
arquivos <- glue("{dados_path}/{list.files(dados_path)}")

length(arquivos)
bytes <- file.size(arquivos) %>% sum()
bytes / 1024^2 #> 7588,352 Megabytes
bytes / 1024^3 #> 7,4105 Gigabytes

#| object.size(primeiro) / 1024^2 #> 233,8 Megabytes
#| file.size(arquivo1) / 1024^2 #> 244,8 Megabytes

## Item d)
unique(primeiro$vacina_nome)

janssen <- vroom(
    pipe(glue("grep -i JANSSEN {arquivo1}")),
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE,
    col_names = names(primeiro)
)

### Confirmando que as únicas observações importadas são aquelas
### cuja vacina aplicada foi a Janssen.
unique(janssen$vacina_nome)


pacman::p_load("pryr", "microbenchmark")
object_size(janssen) # [1] 17.55 kB
object_size(primeiro) # [1] 17.55 kB

microbenchmark(
    "pryr" = pryr::object_size(primeiro),
    "utils" = utils::object.size(primeiro),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Pacote", "Mínimo", "Média", "Máximo")) %>%
    kable(digits = 4)

RAM_primeiro <- object.size(primeiro) / 1024^2 #> 233,8 Megabytes
RAM_janssen <- object.size(janssen) / 1024^2 #> 6,6 Megabytes
RAM_primeiro - RAM_janssen #> 227,1 Megabytes

## Item e)
todos <- vroom(arquivos,
    delim = ";",
    num_threads = 7
)