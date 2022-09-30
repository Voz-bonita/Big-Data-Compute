if (!require(pacman)) install.packages("pacman")
pacman::p_load("vroom", "glue", "geobr", "rjson", "dplyr")


arquivos <- list.files(".\\Lista_1\\dados", full.names = TRUE)
vax_all <- vroom(
    arquivos,
    col_select = c(
        "estabelecimento_uf",
        "vacina_descricao_dose",
        "estabelecimento_municipio_codigo"
    ),
    show_col_types = FALSE,
    col_types = list("estabelecimento_municipio_codigo" = col_character()),
    num_threads = 7
)


codigos <- vroom(
    "./Lista_2_gabarito/Tabela_codigos.csv",
    col_types = list("Cód IBGE" = col_character())
) %>%
    select(c("Cód IBGE", "Nome da Região de Saúde")) %>%
    rename_all(~ c(
        "est_mun_codigo", "name_health_region"
    ))