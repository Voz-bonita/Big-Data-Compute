# \ devtools::install_github("ipeaGIT/geobr", subdir = "r-package")
pacman::p_load("data.table", "glue", "geobr", "dplyr", "vroom")


### Preparativos da Questão 1
dados_path <- "./Lista_1/dados"
arquivos <- glue("{dados_path}/{list.files(dados_path)}")
primeiro <- vroom(arquivos[1],
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE
)

# Questão 2
## item a)
seg_dose <- fread(
    glue('grep -i "2Âª Dose" {dados_path}/*.csv'),
    col.names = names(primeiro)
)[
    ,
    c(
        "estabelecimento_uf",
        "vacina_descricao_dose",
        "estabelecimento_municipio_codigo"
    )
]
ncol(seg_dose) #> 3 colunas
nrow(seg_dose) #> 5861457 linhas

regioes_saude <- read_health_region()