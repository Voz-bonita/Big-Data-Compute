pacman::p_load("dplyr", "stringr", "glue")

n_prime <- function(n) {
    "
    Retorna os n primeiros números primos
    começando do 2
    "
    primes <- numeric(n)
    primes[1] <- 2
    k <- 2
    i <- 3
    while (primes[n] == 0) {
        prime <- T
        for (j in 2:(i - 1)) {
            if (i %% j == 0) {
                prime <- F
                break
            }
        }
        if (prime) {
            primes[k] <- i
            k <- k + 1
        }
        i <- i + 1
    }
    return(primes)
}

prob_jogo <- function(time_a, time_b, params, n_simul = 10^6) {
    "
    Função responsável por, dados dois times e o resultado
    da partida (vitória do time a, empate ou derrota do time a)
    retorna a probabilidade desse resultado ocorrer
    "
    gols_a <- rpois(n_simul, params[[time_a]][time_b])
    gols_b <- rpois(n_simul, params[[time_b]][time_a])

    probs <- c(
        "V" = sum(gols_a > gols_b),
        "E" = sum(gols_a == gols_b),
        "D" = sum(gols_a < gols_b)
    ) / n_simul

    return(probs)
}

expand_probs <- function(paises, codificado, params) {
    jogos_possiveis <- combn(paises, 2)
    n_jogos <- ncol(jogos_possiveis)
    probs <- c()
    for (i in 1:n_jogos) {
        time_a <- jogos_possiveis[1, i]
        time_b <- jogos_possiveis[2, i]

        identificador <- codificado[time_b] * log(codificado[time_a])

        probs_i <- prob_jogo(time_a, time_b, params = params)
        probs[as.character(identificador)] <- probs_i["V"]
        probs[as.character(2 * identificador)] <- probs_i["E"]
        probs[as.character(3 * identificador)] <- probs_i["D"]
    }
    return(probs)
}

simul_grupo <- function(grupo, codificado, pre_probs) {
    grupo_cod <- codificado[grupo]

    # cenarios := ("V", "E", "D")
    cenarios <- c(1, 2, 3)

    ### Sistema de pontuação da Copa do Mundo FIFA
    time_a <- c("V" = 3, "E" = 1, "D" = 0)
    time_b <- c("V" = 0, "E" = 1, "D" = 3)

    #### Jogos em seus indices
    jogos <- combn(grupo_cod, 2)
    jogos <- jogos[2, ] * log(jogos[1, ])


    resultados <- expand.grid(rep(list(cenarios), length(jogos))) %>%
        rename_all(~ glue("Jogo{1:(length(jogos))}")) %>%
        mutate("{grupo[1]}" := time_a[Jogo1] + time_a[Jogo2] + time_a[Jogo3]) %>%
        mutate("{grupo[2]}" := time_b[Jogo1] + time_a[Jogo4] + time_a[Jogo5]) %>%
        mutate("{grupo[3]}" := time_b[Jogo2] + time_b[Jogo4] + time_a[Jogo6]) %>%
        mutate("{grupo[4]}" := time_b[Jogo3] + time_b[Jogo5] + time_b[Jogo6]) %>%
        mutate(
            Prob =
                probs[as.character(Jogo1 * jogos[1])] *
                    probs[as.character(Jogo2 * jogos[2])] *
                    probs[as.character(Jogo3 * jogos[3])] *
                    probs[as.character(Jogo4 * jogos[4])] *
                    probs[as.character(Jogo5 * jogos[5])] *
                    probs[as.character(Jogo6 * jogos[6])]
        )

    return(resultados)
}