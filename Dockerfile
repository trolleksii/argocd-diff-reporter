FROM golang:1.25.7 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o argocd-diff-reporter ./cmd/main.go

FROM golang:1.25.7
COPY --from=build /app/argocd-diff-reporter /bin/argocd-diff-reporter
ENTRYPOINT ["/bin/argocd-diff-reporter"]
