locals {
  repositories = ["chronicle", "chronicle-victim"]
}

resource "aws_ecr_repository" "this" {
  for_each = toset(local.repositories)

  name                 = each.key
  image_tag_mutability = "MUTABLE"
  force_delete         = true # destroy removes the images too

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = aws_ecr_repository.this
  repository = each.value.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}
