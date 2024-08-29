FROM rocker/geospatial:4.2

# Install imports
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    git2r \
    googleAuthR \
    googleCloudStorageR \
    logger \
    magrittr \
    purrr \
    arrow \
    stringr \
    tidyr \
    lubridate \
    rlang

# Install suggests
RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    sessioninfo \
    tibble \
    roxygen2 \
    tidyselect \
    remotes \
    tidytext \
    KoboconnectR \
    univOutl \
    taxize \
    reticulate \
    stringi

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
