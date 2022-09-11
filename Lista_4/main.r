if (!require(pacman)) install.packages("pacman")
pacman::p_load("readxl", "dplyr", "stringr", "purrr")
set.seed(2022)


serie <- read_xlsx(
    "./Lista_4/Estatisticas_times.xlsx",
    col_types = c("text", "numeric", "numeric", "numeric")
) %>%
    mutate(GF = Gols_feitos / Jogos) %>%
    mutate(GS = Gols_sofridos / Jogos) %>%
    mutate(Pais = str_sub(Pais, 2L, -1L))

#--- Lista de parâmetros ---#
gf <- pull(serie, GF, name = Pais)
gs <- pull(serie, GS, name = Pais)
lambda_ij <- map(gf, ~ (.x + gs) / 2)
#--- #

# Item a)
#--- Probabilidade exata ~~ 0.00434 ~~ 0.43%
dpois(5, lambda_ij[["Brazil"]]["Serbia"]) *
    dpois(0, lambda_ij[["Serbia"]]["Brazil"])


n <- 10^6
br <- rpois(n, lambda_ij[["Brazil"]]["Serbia"])
serv <- rpois(n, lambda_ij[["Serbia"]]["Brazil"])
sum(br == 5 & serv == 0) / n
#--- Probabilidade estimada ~~ 0.00418 ~~ 0.42%