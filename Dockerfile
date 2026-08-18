FROM golang:1.25-alpine3.23 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o argocd-diff-reporter ./cmd/main.go

FROM alpine:3.23
RUN apk add --no-cache git ca-certificates
COPY --from=build /app/argocd-diff-reporter /bin/argocd-diff-reporter
ENTRYPOINT ["/bin/argocd-diff-reporter"]
