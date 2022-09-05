pacman::p_load(
    "arrow", "dplyr", "stringr", "sparklyr",
    "lubridate", "ggplot2", "glue", "purrr"
)


files <- list.files("./Lista_3/parquets", full.names = T)
file.copy(
    files[str_detect(files, "(20)(20|19|18|17|16)")],
    "./Lista_3/2016_2020"
)
files[str_detect(files, "(20)(20|19|18|17|16)")]


sc <- spark_connect(master = "local", version = "2.4.3")
sinasc <- spark_read_parquet(
    sc,
    path = "Lista_3/2016_2020/*"
) %>%
    mutate(DTNASC = from_unixtime(unix_timestamp(DTNASC, "ddMMyyyy"), "u")) %>%
    mutate(CONSULTAS = as.numeric(CONSULTAS)) %>%
    mutate(DTNASC = as.numeric(DTNASC)) %>%
    mutate(ESCMAE = as.numeric(ESCMAE)) %>%
    mutate(ESTCIVMAE = as.numeric(ESTCIVMAE)) %>%
    mutate(GESTACAO = as.numeric(GESTACAO)) %>%
    mutate(GRAVIDEZ = as.numeric(GRAVIDEZ)) %>%
    mutate(IDADEMAE = as.numeric(IDADEMAE)) %>%
    mutate(PARTO = as.numeric(PARTO)) %>%
    mutate(PESO = as.numeric(PESO)) %>%
    mutate(QTDFILMORT = as.numeric(QTDFILMORT)) %>%
    mutate(QTDFILVIVO = as.numeric(QTDFILVIVO)) %>%
    mutate(SEXO = as.numeric(SEXO)) %>%
    mutate(LOCNASC = as.numeric(LOCNASC)) %>%
    mutate(CODMUNNASC = as.numeric(CODMUNNASC)) %>%
    mutate(CODMUNRES = as.numeric(CODMUNRES)) %>%
    mutate(APGAR1 = as.numeric(APGAR1)) %>%
    mutate(APGAR5 = as.numeric(APGAR5)) %>%
    mutate(RACACOR = as.numeric(RACACOR)) %>%
    mutate(CODANOMAL = as.numeric(CODANOMAL))

sinasc_copia <- mutate(sinasc, PARTO = if_else(PARTO != 2, 1, 0, 0))

linhas <- sdf_dim(sinasc_copia)[1]
faltante <- sinasc_copia %>%
    summarise_all(~ sum(as.integer(is.na(.)) / linhas)) %>%
    collect()

sinasc_copia <- sinasc_copia %>%
    select(-colnames(faltante[which(faltante > 0.10)]))

#--------- Imputação e correções ---------#
qnt_vars <- c(
    "IDADEMAE", "QTDFILVIVO", "QTDFILMORT",
    "PESO", "APGAR1", "APGAR5"
)

sinasc_copia <- sinasc_copia %>%
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
    filter(!is.na(DTNASC)) %>%
    mutate(CONSULTAS = if_else(is.na(CONSULTAS), 9, CONSULTAS)) %>%
    ft_one_hot_encoder(
        input_cols = "DTNASC",
        output_cols = "DTNASC_dummy"
    ) %>%
    ft_binarizer(
        input_col = "CONSULTAS",
        output_col = "CONSULTAS_bin",
        threshold = 3
    ) %>%
    mutate(GRAVIDEZ = if_else(is.na(GRAVIDEZ), 9, GRAVIDEZ)) %>%
    mutate(GESTACAO = if_else(is.na(GESTACAO), 9, GESTACAO)) %>%
    mutate(ESCMAE = if_else(is.na(ESCMAE), 9, ESCMAE)) %>%
    mutate(ESTCIVMAE = if_else(is.na(ESTCIVMAE), 9, ESTCIVMAE)) %>%
    mutate(RACACOR = if_else(is.na(RACACOR), 9, RACACOR))


#--------- Modelo de Regressão ---------#
#--- Preparativos ---#
particoes <- sinasc_copia %>%
    sdf_random_split(training = 0.8, test = 0.2, seed = 2022)

treino <- particoes$training
teste <- particoes$test

#--- Treino ---#
label_out <- "PARTO"
features <- paste(
    colnames(select(
        sinasc_copia,
        -c(all_of(qnt_vars), "qnt_features", "DTNASC", "CONSULTAS", "PARTO")
    )),
    collapse = " + "
)
formula <- (glue("{label_out} ~ {features}"))
lr_model <- ml_logistic_regression(treino, formula, max_iter = 100)

#--- Validação ---#
validation_summary <- ml_evaluate(lr_model, teste)

validation_summary$area_under_roc()

roc <- validation_summary$roc() %>%
    collect()

roc <- read.csv("./Lista_3/roc.csv")

ggplot(roc, aes(x = FPR, y = TPR)) +
    geom_line(size = 1, color = "lightgreen") +
    geom_abline(lty = "dashed") +
    theme_bw()