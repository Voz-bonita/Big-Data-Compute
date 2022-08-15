if (!require(pacman)) install.packages("pacman")
pacman::p_load("mongolite")
source("./Lista_2/banco.r")

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