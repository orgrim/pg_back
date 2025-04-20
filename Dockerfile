# syntax=docker/dockerfile:1

# Build the application from source
FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:latest AS build-stage

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /go/bin/pg_back

# Deploy the application binary into a lean image
FROM --platform=${TARGETPLATFORM:-linux/amd64} alpine:latest AS build-release-stage

WORKDIR /app

RUN apk add --no-cache postgresql-client

COPY --from=build-stage /go/bin/pg_back /app/pg_back

ENTRYPOINT ["/app/pg_back"]
