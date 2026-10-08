FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/conveyor-api ./cmd/conveyor-api && CGO_ENABLED=0 go build -o /out/harness-worker ./cmd/harness-worker && CGO_ENABLED=0 go build -o /out/demoload ./cmd/demoload

FROM gcr.io/distroless/static
COPY --from=build /out/conveyor-api /usr/local/bin/conveyor-api
COPY --from=build /out/harness-worker /usr/local/bin/harness-worker
COPY --from=build /out/demoload /usr/local/bin/demoload
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/conveyor-api"]
