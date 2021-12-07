FROM golang:1.16-bullseye

RUN apt update && apt install -y git gcc

RUN git clone https://github.com/ISE-SMILE/corral-tpc-h.git
WORKDIR corral-tpc-h
RUN go build -o cortpch

CMD ["./cortpch","-config","/config.json"]