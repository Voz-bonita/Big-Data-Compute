if (!require(pacman)) install.packages("pacman")
pacman::p_load("arrow", "dplyr", "stringr", "sparklyr", "lubridate")
source("./Lista_1/funcoes_aux.r")

sc <- spark_connect(master = "local", version = "2.4.3")

# Questão 2

sinasc <- spark_read_parquet(
    sc,
    path = "Lista_3/teste/*"
) %>%
    select(-c(
        contador, LOCNASC, CODMUNNASC,
        CODMUNRES, APGAR1, APGAR5,
        RACACOR, CODANOMAL, PESO,
    )) %>%
    mutate(DTNASC = from_unixtime(unix_timestamp(DTNASC, "ddMMyyyy"), "u")) %>%
    mutate(CODOCUPMAE = as.numeric(CODOCUPMAE)) %>%
    mutate(CONSULTAS = as.numeric(CONSULTAS)) %>%
    mutate(DTNASC = as.numeric(DTNASC)) %>%
    mutate(ESCMAE = as.numeric(ESCMAE)) %>%
    mutate(ESTCIVMAE = as.numeric(ESTCIVMAE)) %>%
    mutate(GESTACAO = as.numeric(GESTACAO)) %>%
    mutate(GRAVIDEZ = as.numeric(GRAVIDEZ)) %>%
    mutate(IDADEMAE = as.numeric(IDADEMAE)) %>%
    mutate(PARTO = as.numeric(PARTO)) %>%
    mutate(QTDFILMORT = as.numeric(QTDFILMORT)) %>%
    mutate(QTDFILVIVO = as.numeric(QTDFILVIVO)) %>%
    mutate(SEXO = as.numeric(SEXO))

sinasc %>%
    select(IDADEMAE, QTDFILVIVO, QTDFILMORT) %>%
    sdf_describe() %>%
    mutate_at(c("IDADEMAE", "QTDFILVIVO", "QTDFILMORT"), as.numeric) %>%
    rename_all(
        ~ c(
            "Medida", "Idade da Mãe",
            "Nº de Filhos vivos", "Nº de Filhos mortos"
        )
    ) %>%
    format_tab(
        cap = "Medidas descritivas das variáveis quantitivas do SINASC",
        digits = 2,
        "latex"
    )

sinasc %>%
    summarise_all(~ sum(as.integer(is.na(.))))

qnt_vars <- c("IDADEMAE", "QTDFILVIVO")

sinasc %>%
    ft_imputer(
        input_cols = qnt_vars,
        output_cols = qnt_vars,
        strategy = "median"
    ) %>%
    ft_vector_assembler(
        input_cols = qnt_vars,
        output_col = "qnt_features"
    ) %>%
    ft_standard_scaler(
        input_col = "qnt_features",
        output_col = "standard_qnt"
    ) %>%
    ft_one_hot_encoder(input_cols = "DTNASC", output_cols = as.character(1:7L))

sinasc %>%
    filter(!is.na(DTNASC)) %>%
    ft_one_hot_encoder(
        input_cols = "DTNASC",
        output_cols = "dummyDTNASC"
    )