if (!require(pacman)) install.packages("pacman")
pacman::p_load(
    "arrow", "dplyr", "stringr", "sparklyr",
    "lubridate", "ggplot2", "glue", "purrr"
)
source("./Lista_1/funcoes_aux.r")

sc <- spark_connect(master = "local", version = "2.4.3")

# Questão 2
#--------- Carrega e trata a db ---------#
sinasc <- spark_read_parquet(
    sc,
    path = "Lista_3/parquets/*"
) %>%
    select(-c(
        contador, LOCNASC, CODMUNNASC,
        CODMUNRES, APGAR1, APGAR5,
        RACACOR, CODANOMAL
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
    mutate(PESO = as.numeric(PESO)) %>%
    mutate(QTDFILMORT = as.numeric(QTDFILMORT)) %>%
    mutate(QTDFILVIVO = as.numeric(QTDFILVIVO)) %>%
    mutate(SEXO = as.numeric(SEXO))

sinasc <- mutate(sinasc, PARTO = if_else(PARTO != 2, 1, 0, 0))
#--------- Exploratória ---------#
sdf_dim(sinasc)
glimpse(sinasc)

sinasc %>%
    select(IDADEMAE, QTDFILVIVO, QTDFILMORT, PESO) %>%
    sdf_describe() %>%
    mutate_at(c("IDADEMAE", "QTDFILVIVO", "QTDFILMORT", "PESO"), as.numeric) %>%
    rename_all(
        ~ c(
            "Medida", "Idade da Mãe",
            "Nº de Filhos vivos", "Nº de Filhos mortos",
            "Peso"
        )
    ) %>%
    format_tab(
        cap = "Medidas descritivas das variáveis quantitivas do SINASC",
        digits = 2,
        "latex"
    )

### Pega a frequência relativa de NAs em cada variável
### e decide sobre manter ou não com base em um threshold de 10%
linhas <- sdf_dim(sinasc)[1]
faltante <- sinasc %>%
    summarise_all(~ sum(as.integer(is.na(.)) / linhas)) %>%
    collect()

sinasc <- sinasc %>%
    select(-colnames(faltante[which(faltante > 0.10)]))


covariancia_freq <- function(covar) {
    sinasc %>%
        group_by_at(c(covar, "PARTO")) %>%
        tally() %>%
        mutate(frac = round(n / sum(n), 2)) %>%
        arrange_at(c(covar, "PARTO")) %>%
        collect() %>%
        as.data.frame()
}

walk(colnames(sinasc), ~ print(covariancia_freq(.x)))

covariancia_freq("IDADEMAE") %>%
    filter(!(IDADEMAE %in% c(0, 99))) %>%
    ggplot(aes(x = IDADEMAE, y = frac)) +
    geom_histogram(aes(fill = PARTO), stat = "identity", position = "stack") +
    theme_bw() +
    xlab("Idade da mãe") +
    ylab("Frequência Relativa (%)")

covariancia_freq("GESTACAO") %>%
    ggplot(aes(x = GESTACAO, y = frac)) +
    geom_histogram(aes(fill = PARTO), stat = "identity", position = "stack") +
    scale_x_discrete() +
    theme_bw() +
    xlab("Gestação") +
    ylab("Frequência Relativa (%)") +
    scale_x_continuous(breaks = 1:9L, labels = c(
        "Menos de 22 semanas",
        "22 a 27 semanas",
        "28 a 31 semanas",
        "32 a 36 semanas",
        "37 a 41 semanas",
        "42 semanas e mais",
        "",
        "Erro de registro",
        "Ignorado"
    ), ) +
    theme(axis.text.x = element_text(angle = 60, hjust = 1))


#--------- Imputação e correções ---------#
qnt_vars <- c("IDADEMAE", "QTDFILVIVO", "PESO")

sinasc <- sinasc %>%
    ft_imputer(
        input_cols = qnt_vars,
        output_cols = qnt_vars,
        strategy = "median"
    ) %>%
    filter(!is.na(DTNASC)) %>%
    mutate(CONSULTAS = if_else(is.na(CONSULTAS), 9, CONSULTAS)) %>%
    mutate(GRAVIDEZ = if_else(is.na(GRAVIDEZ), 9, GRAVIDEZ)) %>%
    mutate(GESTACAO = if_else(is.na(GESTACAO), 9, GESTACAO)) %>%
    mutate(ESCMAE = if_else(is.na(ESCMAE), 9, ESCMAE))

sinasc <- sinasc %>%
    ft_vector_assembler(
        input_cols = qnt_vars,
        output_col = "qnt_features"
    ) %>%
    ft_standard_scaler(
        input_col = "qnt_features",
        output_col = "standard_qnt"
    )

sinasc <- sinasc %>%
    ft_one_hot_encoder(
        input_cols = "DTNASC",
        output_cols = "DTNASC_dummy"
    ) %>%
    ft_binarizer(
        input_col = "CONSULTAS",
        output_col = "CONSULTAS_bin",
        threshold = 3
    )


#--------- Modelo de Regressão ---------#
#--- Preparativos ---#
particoes <- sinasc %>%
    sdf_random_split(training = 0.8, test = 0.2, seed = 2022)

treino <- particoes$training
teste <- particoes$test

#--- Treino ---#
label_out <- "PARTO"
features <- paste(
    colnames(select(
        sinasc,
        -c(qnt_vars, "qnt_features", "DTNASC", "CONSULTAS", "PARTO")
    )),
    collapse = " + "
)
formula <- (glue("{label_out} ~ standard_qnt + QTDFILVIVO + GESTACAO + ESCMAE"))
lr_model <- ml_logistic_regression(treino, formula, max_iter = 10)

#--- Validação ---#
validation_summary <- ml_evaluate(lr_model, teste)

validation_summary$area_under_roc()

roc <- validation_summary$roc() %>%
    collect()

ggplot(roc, aes(x = FPR, y = TPR)) +
    geom_line() +
    geom_abline(lty = "dashed")