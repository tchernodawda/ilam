FROM flink:1.18-scala_2.12

# Installer Python3 et PyFlink de façon permanente
USER root
RUN apt-get update -qq && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.18.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER flink
