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
    stringr \
    tidyr \
    rlang \
    lubridate \
    arrow

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
    reticulate \
    stringi \
    taxize

# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
