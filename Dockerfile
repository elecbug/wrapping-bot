# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.26.5

FROM golang:${GO_VERSION}-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wrapping-botd ./cmd/wrapping-botd \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wrapping-bot ./cmd/wrapping-bot

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/wrapping-botd /wrapping-botd
COPY --from=build /out/wrapping-bot /wrapping-bot
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/wrapping-botd"]
