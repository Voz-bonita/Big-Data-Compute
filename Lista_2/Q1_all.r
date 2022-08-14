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


seg_dose_query <- "SELECT * FROM vax
                   WHERE (vacina_descricao_dose='2ª Dose'
                   OR vacina_descricao_dose='2ª Dose Revacinação')"

fast_join <- glue("SELECT
                    (SELECT name_health_region
                        FROM codigos
                        WHERE (codigos.est_mun_codigo =
                            seg_dose.estabelecimento_municipio_codigo)
                            ) AS regiao_saude
                   FROM ({seg_dose_query}) AS seg_dose")

qnt_vax_query <- glue("SELECT regiao_saude, COUNT(*) AS N
                       FROM ({fast_join})
                       GROUP BY regiao_saude")

faixa_query <- glue("SELECT regiao_saude, N,
                        CASE WHEN N > (SELECT MEDIAN(N) FROM ({qnt_vax_query}))
                            THEN 'Alto'
                            ELSE 'Baixo'
                            END AS Faixa
                    FROM qnt_vax")

bot5_query <- glue("WITH tabFaixa AS ({faixa_query})
                    SELECT *
                    FROM (SELECT * FROM tabFaixa ORDER BY N ASC LIMIT 5)
                    UNION
                    SELECT *
                    FROM (SELECT * FROM tabFaixa ORDER BY N DESC LIMIT 5)
                    ORDER BY N")

ans <- dbGetQuery(sql_conn, bot5_query)

dbDisconnect(sql_conn)


## Item c)
mongo_local <- function(collection) {
    conn <- mongo(
        collection = collection,
        db = "Lista2_db",
        url = "mongodb://localhost"
    )
    return(conn)
}

conn_vax <- mongo_local("vax")
conn_vax$insert(vax_all)

conn_codigos <- mongo_local("codigos")
conn_codigos$insert(codigos)

qnt_vax <- conn_vax$aggregate('[
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
            {"vacina_descricao_dose": "2Âª Dose"},
            {"vacina_descricao_dose": "2Âª Dose RevacinaÃ§Ã£o"}
            ]
        }
    },
    { "$group": {
        "_id": "$name_health_region",
        "N": {"$sum": 1}
        }
    }
]') %>%
    rename("Nome" = `_id`)

conn_qnt_vax <- mongo_local("qnt_vax")
conn_qnt_vax$insert(qnt_vax)

mediana <- mediana(conn_qnt_vax$find()$N)
conn_qnt_vax$update(
    query = "{}",
    update = glue('[{{
        "$set": {{
            "Faixa": {{
                "$switch": {{
                    "branches": [
                        {{ "case":
                            {{"$gte": ["$N", {mediana}]}},
                            "then": "Alto" }},
                        {{ "case":
                            {{"$lt": ["$N", {mediana}]}},
                            "then": "Baixo" }}
                    ]
                }}
            }}
        }}
    }}]'),
    multiple = TRUE,
    upsert = TRUE
)

n <- 5
bot5 <- bind_rows(
    conn_qnt_vax$find(
        limit = n, sort = '{"N": 1}'
    ),
    conn_qnt_vax$find(
        skip = conn_qnt_vax$count() - n,
        limit = n, sort = '{"N": 1}'
    )
)