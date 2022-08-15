if (!require(pacman)) install.packages("pacman")
pacman::p_load("vroom", "glue", "geobr", "rjson", "dplyr")


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

reg_saude <- read_health_region()
ibge_geobr <- fromJSON(file = "./Lista_1/mapa_codigos.json") %>% unlist()
codigos <- data.frame(
    "est_mun_codigo" = names(ibge_geobr),
    "code_health_region" = ibge_geobr
) %>%
    merge(reg_saude, by = "code_health_region") %>%
    select(est_mun_codigo, name_health_region)