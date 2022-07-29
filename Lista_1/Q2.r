# \ devtools::install_github("ipeaGIT/geobr", subdir = "r-package")
pacman::p_load("data.table", "glue", "geobr", "dplyr")


# Questão 2
## item a)
dados_path <- "./Lista_1/dados"
arquivos <- glue("{dados_path}/{list.files(dados_path)}")

dados <- lapply(arquivos[1:3], fread,
    sep = ";",
    select = c(
        "estabelecimento_uf",
        "vacina_descricao_dose",
        "estabelecimento_municipio_codigo"
    )
) %>%
    rbindlist()

regios_saude <- read_health_region()