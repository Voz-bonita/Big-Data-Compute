pacman::p_load("vroom", "knitr", "dplyr", "glue", "tidyr", "kableExtra")
source("./Lista_1/funcoes_aux.r")

# Questão 1
## Item b)
dados_path <- "./Lista_1/dados"
arquivo1 <- glue("{dados_path}/AC-Parte_1.csv")
primeiro <- vroom(arquivo1,
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE
)
ncol(primeiro) #> 32 colunas
nrow(primeiro) #> 499890 linhas

head(primeiro[3:6L]) %>%
    format_tab(
        caption = "\\label{tab:1b}Cinco primeiras observações da
        primeira parte dos dados de vacinação do estado do Acre."
    )

## Item c)
arquivos <- glue("{dados_path}/{list.files(dados_path)}")

length(arquivos) #> 12 arquivos
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

ncol(janssen) #> 32 colunas
nrow(janssen) #> 13222 linhas
### O banco "janssen" conta com uma redução de 486668 linhas em relação ao "primeiro"

head(janssen[3:6L]) %>%
    format_tab(
        caption = "\\label{tab:1d1}Cinco primeiras observações da primeira
        parte dos dados de vacinação do estado do Acre,
        cuja vacina aplicada foi a Janssen."
    )

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
    format_tab(
        caption = "\\label{tab:1d2}Benchmark do tempo de execução das funções
        object\\_size e object.size sobre o banco AC-Parte\\_1.",
        digits = 4
    )

RAM_primeiro <- object.size(primeiro) / 1024^2 #> 233,8 Megabytes
RAM_janssen <- object.size(janssen) / 1024^2 #> 6,6 Megabytes
RAM_primeiro - RAM_janssen #> 227,1 Megabytes

## Item e)
todos <- vroom(
    pipe(glue("grep -i JANSSEN {dados_path}/*.csv")),
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE,
    col_names = names(primeiro)
)

ncol(todos) #> 32 colunas
nrow(todos) #> 316265 > linhas

### Preparação para a questão 2
sort(unique(todos$vacina_descricao_dose))