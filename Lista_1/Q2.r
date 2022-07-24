pacman::p_load("data.table", "glue")


# Questão 2
## item a)
dados_path <- "./Lista_1/dados"
arquivos <- glue("{dados_path}/{list.files(dados_path)}")

dados <- lapply(arquivos[1], fread, sep = ";", select = 1:3L) %>%
    rbindlist()