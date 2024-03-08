#
# Note: this won't contain any of the Spark machinery, it's only used to run `turbine_cmd` on non-native platforms
# like macOS.
#
# To build:
#   docker build . -t myubuntu
#
FROM ubuntu:22.04
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker
RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update \
  && apt-get install -y ca-certificates \
  && rm -rf /var/lib/apt/lists/*
