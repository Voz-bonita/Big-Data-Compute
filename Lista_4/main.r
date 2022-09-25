if (!require(pacman)) install.packages("pacman")
pacman::p_load("readxl", "dplyr", "stringr", "purrr", "glue")
source("./Lista_4/custom_functions.r")
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

# ------- Probabilidade de qualquer jogo da Copa
paises <- pull(serie, Pais)
codificado <- n_prime(length(paises))
names(codificado) <- paises

probs <- expand_probs(paises, codificado, lambda_ij)
# -------

# Item b)
grupo_g <- c("Brazil", "Cameroon", "Serbia", "Switzerland")
expand_grp_g <- simul_grupo(grupo_g, codificado, probs)

### Se a pontuação do Brasil estiver em pelo menos segundo, ele passa
bra_next <- expand_grp_g %>%
    rowwise() %>%
    mutate(Aprovacao = Brazil %in%
        tail(sort(c(Brazil, Cameroon, Serbia, Switzerland)), 2)) %>%
    filter(Aprovacao)

prob_by_win(1) #> ~= 42.06%
prob_by_win(2) #> ~= 42.09%
prob_by_win(3) #> ~= 48.11%