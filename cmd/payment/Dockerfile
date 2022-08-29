###############
# BUILD   ENV #
###############
FROM golang:1.17 as builder
WORKDIR "/builder"
COPY . ./
RUN cd cmd/payment/ && \ 
    CGO_ENABLED=1 GOOS=linux go build -mod=vendor -o appp ./

###############
# RUN     ENV #
###############
FROM debian:stable-slim
# Set the timezone and add certificates
ENV TZ=Europe/Sofia
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates

WORKDIR /app/
RUN mkdir docs
# Copy the Pre-built binary file from the previous stage
COPY --from=builder /builder/cmd/payment/appp  .
COPY --from=builder /builder/config.yml  .
CMD ["./appp"]
