FROM rocker/geospatial:4.5

RUN apt-get update -qq && apt-get -y --no-install-recommends install \
    libxml2-dev \
    libcairo2-dev \
    libgit2-dev \
    default-libmysqlclient-dev \
    libpq-dev \
    libsasl2-dev \
    libsqlite3-dev \
    libssh2-1-dev \
    unixodbc-dev && \
  rm -rf /var/lib/apt/lists/*


RUN install2.r --error --skipinstalled \
    remotes \
    config \
    dplyr \
    git2r \
    logger \
    magrittr \
    purrr \
    stringr \
    tidyr \
    rlang \
    lubridate \
    arrow \
    cleaner \
    httr2 \
    readr \
    googlesheets4 \
    digest \
    furrr \
    future \
    mongolite \
    rfishbase

RUN install2.r --error --skipinstalled \
    covr \
    pkgdown \
    sessioninfo \
    tibble \
    roxygen2 \
    tidyselect \
    tidytext \
    KoboconnectR \
    univOutl \
    reticulate \
    stringi \
    taxize

# Install GitHub packages
RUN Rscript -e 'remotes::install_github("WorldFishCenter/peskas.coasts", ref = "2.2.6")'

# Install local package
COPY . /home
WORKDIR /home
RUN Rscript -e 'remotes::install_local(dependencies = TRUE)'

ENTRYPOINT ["Rscript"]