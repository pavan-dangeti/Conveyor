FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN for cmd in conveyor-api harness-worker conveyor; do \
      go build -trimpath -ldflags="-s -w" -o /out/$cmd ./cmd/$cmd || exit 1; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/conveyor-api"]
