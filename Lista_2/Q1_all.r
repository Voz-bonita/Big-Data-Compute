if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "vroom", "glue", "DBI", "RSQLite",
    "geobr", "rjson", "dplyr", "mongolite"
)


# Questão 1
## Item a)
dados_path <- ".\\Lista_1\\dados"
arquivos <- glue("{dados_path}\\{list.files(dados_path)}")
vax_all <- vroom(
    pipe(glue("type {dados_path}\\*.csv")),
    col_select = c(
        "estabelecimento_uf",
        "vacina_descricao_dose",
        "estabelecimento_municipio_codigo"
    ),
    show_col_types = FALSE,
    col_types = list("estabelecimento_municipio_codigo" = col_character()),
    num_threads = 7
)

sql_conn <- dbConnect(RSQLite::SQLite(), "./Lista_2/lista2.sqlite")
dbWriteTable(sql_conn, "vax", vax_all)


# Questão 2
## Item b)
reg_saude <- read_health_region()
ibge_geobr <- fromJSON(file = "./Lista_1/mapa_codigos.json") %>% unlist()
codigos <- data.frame(
    "est_mun_codigo" = names(ibge_geobr),
    "code_health_region" = ibge_geobr
) %>%
    merge(reg_saude, by = "code_health_region") %>%
    select(est_mun_codigo, name_health_region)

dbWriteTable(sql_conn, "codigos", codigos)


join_query <- "SELECT janssen_all.estabelecimento_uf,
                    janssen_all.vacina_descricao_dose,
                    codigos.name_health_region
                FROM janssen_all
                LEFT JOIN codigos
                ON janssen_all.estabelecimento_municipio_codigo =
                    codigos.est_mun_codigo"

seg_dose_query <- glue("SELECT * FROM ({join_query}) AS SUBQUERY
                        WHERE (vacina_descricao_dose='2ª Dose')")

qnt_vax_query <- glue("SELECT name_health_region, COUNT(*) AS N
                       FROM ({seg_dose_query})
                       GROUP BY name_health_region")

ans <- dbGetQuery(sql_conn, qnt_vax_query)

dbDisconnect(sql_conn)


## Item c)
conn_vax <- mongo(
    collection = "vax",
    db = "Lista2_db",
    url = "mongodb://localhost"
)
conn_vax$insert(vax_all)

conn_codigos <- mongo(
    collection = "codigos",
    db = "Lista2_db",
    url = "mongodb://localhost"
)
conn_codigos$insert(codigos)

joined <- conn_vax$aggregate('[
    {
        "$lookup":
        {
            "from": "codigos",
            "localField": "estabelecimento_municipio_codigo",
            "foreignField": "est_mun_codigo",
            "as": "regDocs"
        }
    },
    {
        "$project":{
            "vacina_descricao_dose": 1,
            "estabelecimento_uf": 1,
            "name_health_region": "$regDocs.name_health_region"
        }
    },
    { "$match": {
        "$or": [
            {"vacina_descricao_dose": "2\u00aa Dose"},
            {"vacina_descricao_dose": "2\u00aa Dose Revacina\u00e7\u00e3o"}
            ]
        }
    },
    { "$group": {
        "_id": "$name_health_region",
        "count": {"$sum": 1}}
    }
]')