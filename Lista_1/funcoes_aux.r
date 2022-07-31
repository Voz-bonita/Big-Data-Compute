pacman::p_load("dplyr", "knitr", "kableExtra")


format_tab <- function(df, caption, ...) {
    tabela <- kable(
        df,
        caption = caption,
        booktabs = T,
        ...
    ) %>%
        kable_styling(
            latex_options = c("striped", "hold_position"),
            full_width = F
        )
    return(tabela)
}

alto_baixo <- function(x) {
    x <- as.double(c(x))

    cond <- x < 0
    x[cond] <- "baixo"
    x[!cond] <- "alto"

    return(x)
}