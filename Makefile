IMAGE_NAME ?= argocd-diff-reporter
IMAGE_TAG ?= latest
REGISTRY ?= ghcr.io/trolleksii
DEPLOYMENT_NAME ?= argocd-diff-reporter
CONTEXT_NAME ?= homelab
NAMESPACE ?= argocd
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: deploy build push restart

deploy: build push restart
	@echo "Deployment complete: $(FULL_IMAGE)"

build:
	@echo "Building Docker image: $(FULL_IMAGE)"
	docker buildx build --platform linux/amd64 -t $(FULL_IMAGE) .

push:
	@echo "Pushing image to registry"
	docker push $(FULL_IMAGE)

restart:
	@echo "Restarting deployment: $(DEPLOYMENT_NAME)"
	kubectl --context=$(CONTEXT_NAME) rollout restart deployment/$(DEPLOYMENT_NAME) -n $(NAMESPACE)
	kubectl --context=$(CONTEXT_NAME) rollout status deployment/$(DEPLOYMENT_NAME) -n $(NAMESPACE)
