if (!require(pacman)) install.packages("pacman")
pacman::p_load("mongolite", "microbenchmark", "dplyr", "glue")
source("./Lista_2_gabarito/banco.r")
source("./Lista_1/funcoes_aux.r")

## Item c)
mongo_local <- function(collection) {
    conn <- mongo(
        collection = collection,
        db = "Lista2_db_gabarito",
        url = "mongodb://localhost"
    )
    return(conn)
}

solve_mongo <- function() {
    conn_qnt_vax$drop()
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
        { "$match": {"vacina_descricao_dose": "1ª Dose"} },
        { "$group": {
            "_id": "$name_health_region",
            "N": {"$sum": 1}
            }
        }
    ]') %>%
        rename("Nome" = `_id`)

    qnt_vax %>%
        rename_all(~ c("reg_saude", "N")) %>%
        conn_qnt_vax$insert()

    mediana <- median(qnt_vax$N)
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
            '{"Faixa": "Baixo"}',
            limit = n, sort = '{"N": 1}'
        ),
        conn_qnt_vax$find(
            '{"Faixa": "Alto"}',
            limit = n, sort = '{"N": 1}'
        )
    )
    return(bot5)
}

conn_vax <- mongo_local("vax")
conn_vax$insert(vax_all)

conn_codigos <- mongo_local("codigos")
conn_codigos$insert(codigos)

conn_qnt_vax <- mongo_local("qnt_vax")

microbenchmark(
    "Mongo" = solve_mongo(),
    times = 3L, unit = "s"
) %>%
    summary() %>%
    select(-`lq`, -uq, -median, -neval) %>%
    rename_all(~ c("Solução", "Mínimo", "Média", "Máximo")) %>%
    format_tab(caption = "Teste", digits = 1L)