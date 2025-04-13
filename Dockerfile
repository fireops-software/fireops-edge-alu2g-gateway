FROM alpine:latest AS build-stage

# get target platform
ARG TARGETOS
ARG TARGETARCH

# install golang
WORKDIR /
RUN wget https://go.dev/dl/go1.23.3.linux-amd64.tar.gz \
    && tar -xzf go1.23.3.linux-amd64.tar.gz \
    && rm go1.23.3.linux-amd64.tar.gz
ENV PATH=$PATH:/go/bin

# set workdir for project
WORKDIR /app
COPY . .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o fireops-edge-alu2g-gateway main.go

# Deploy the application binary into a lean image
FROM alpine:latest AS build-release-stage

WORKDIR /app

COPY --from=build-stage /app/fireops-edge-alu2g-gateway /app

ENTRYPOINT ["/app/fireops-edge-alu2g-gateway"]
