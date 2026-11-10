#!/usr/bin/env bash
# Builds Chronicle and the victim app for the EKS nodes (linux/amd64), pushes
# both to ECR tagged with the current commit, and points deploy/eks at that tag.
# Commit and push the overlay change afterwards; Argo CD rolls it out.
# Only the tag goes into Git; the registry (it names the AWS account) does not.
set -euo pipefail
cd "$(dirname "$0")/.."

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

# The tag must name exactly what is built.
if [ -n "$(git status --porcelain)" ]; then
  echo "uncommitted changes: commit first so the image tag names what is built" >&2
  exit 1
fi
TAG="$(git rev-parse --short HEAD)"

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

docker buildx build --platform linux/amd64 --push \
  -t "$REGISTRY/chronicle:$TAG" -f Dockerfile .
docker buildx build --platform linux/amd64 --push \
  -t "$REGISTRY/chronicle-victim:$TAG" -f victim/Dockerfile .

# Each overlay has exactly one image entry.
perl -pi -e "s/^(\s+newTag:).*/\$1 \"$TAG\"/" \
  deploy/eks/chronicle/kustomization.yaml deploy/eks/victim/kustomization.yaml

echo
echo "Pushed $TAG. Now commit and push deploy/eks; Argo CD syncs it:"
echo "  git commit -am \"deploy: images $TAG\" && git push"
