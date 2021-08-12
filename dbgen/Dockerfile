FROM debian as build

#install deps
RUN apt update && \
    apt install --yes build-essential git && \
    apt-get clean autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

WORKDIR /work
RUN git clone https://github.com/electrum/tpch-dbgen.git

ADD makefile.suite /work/tpch-dbgen/makefile.suite

WORKDIR /work/tpch-dbgen
RUN make -f makefile.suite

FROM debian
WORKDIR /gen
COPY --from=build /work/tpch-dbgen/dbgen /gen
COPY --from=build /work/tpch-dbgen/dists.dss /gen
COPY gen.sh /gen/
ENTRYPOINT ./gen.sh