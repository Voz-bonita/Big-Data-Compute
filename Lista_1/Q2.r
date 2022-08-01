# \ devtools::install_github("ipeaGIT/geobr", subdir = "r-package")
pacman::p_load(
    "data.table", "glue", "geobr", "dplyr",
    "vroom", "Rfast", "rjson"
)
source("./Lista_1/funcoes_aux.r")


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
ibge_geobr <- fromJSON(file = "./Lista_1/mapa_codigos.json") %>%
    unlist()

seg_dose <- fread(
    glue('grep -i "2Âª Dose" {dados_path}/*.csv'),
    col.names = names(primeiro),
    colClasses = "character"
)[
    ,
    c(
        "estabelecimento_uf",
        "vacina_descricao_dose",
        "estabelecimento_municipio_codigo"
    )
][
    ,
    "code_health_region" := ibge_geobr[estabelecimento_municipio_codigo]
]
ncol(seg_dose) #> 3 colunas
nrow(seg_dose) #> 5861457 linhas

regioes_saude <- as.data.table(read_health_region())[
    ,
    !c("abbrev_state", "name_state", "code_state", "geom")
]


## item b)
joined <- regioes_saude[seg_dose,
    on = "code_health_region"
]
format_tab(
    joined[1:6L, 1:4L],
    "Junção dos dados de vacinação e regiões de saúde."
)

qnt_vax <- joined[, .N,
    by = .(code_health_region, name_health_region)
][, "faixa_de_vacinacao" := alto_baixo(N - median(N))]
format_tab(
    qnt_vax[1:6L, ],
    "Quantidade de vacinados por região saúde."
)

bot5_vax <- qnt_vax[, .(
    vacinados = head(sort(N), 5),
    name_health_region = name_health_region[match(head(sort(N), 5), N)]
), by = .(faixa_de_vacinacao)]
format_tab(
    qnt_vax[1:10L, ],
    "5 regiões de saúde com menos vacinados por faixa de vacinação."
)