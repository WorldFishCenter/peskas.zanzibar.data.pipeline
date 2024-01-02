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
    readr \
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
    stringi \
    taxize

#RUN Rscript -e "devtools::install_version('glmmTMB', version = '1.1.5')"
# Rstudio interface preferences
COPY rstudio-prefs.json /home/rstudio/.config/rstudio/rstudio-prefs.json
