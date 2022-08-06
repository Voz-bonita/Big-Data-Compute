pacman::p_load("vroom", "glue", "DBI", "geobr", "rjson", "dplyr")


# Questão 1
## Item a)
dados_path <- "./Lista_1/dados"
arquivos <- glue("{dados_path}/{list.files(dados_path)}")
primeiro <- vroom(arquivos[1], n_max = 0, show_col_types = FALSE)
todos <- vroom(
    pipe(glue("grep -i JANSSEN {dados_path}/*.csv")),
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE,
    col_names = names(primeiro)
)

janssen <- dbConnect(RSQLite::SQLite(), "./Lista_2/Janssen_db.sqlite")
dbWriteTable(janssen, "janssen_all", todos)


# Questão 2
## Item b)
reg_saude <- read_health_region()
ibge_geobr <- fromJSON(file = "./Lista_1/mapa_codigos.json") %>% unlist()
data.frame(
    "est_mun_codigo" = names(ibge_geobr),
    "code_health_region" = ibge_geobr
) %>%
    merge(reg_saude, by = "code_health_region") %>%
    select(est_mun_codigo, code_health_region, name_health_region) %>%
    dbWriteTable(janssen, "codigos", .)


join_query <- "SELECT janssen_all.estabelecimento_uf,
                    janssen_all.vacina_descricao_dose,
                    codigos.name_health_region
                FROM janssen_all
                LEFT JOIN codigos
                ON janssen_all.estabelecimento_municipio_codigo =
                    codigos.est_mun_codigo"

seg_dose_query <- glue("SELECT * FROM ({join_query})
                        WHERE vacina_descricao_dose='2ª Dose' OR
                            vacina_descricao_dose='2ªDose Revacinação'")


dbDisconnect(janssen)