#!/bin/bash

# .env variables should look like this
#AWS_REGION="<aws-region>"
#AWS_ACCOUNT_ID="<aws-account-id>"
#IMAGE_NAME="<image-name>"
#REPOSITORY_NAME="<repository-name>"
# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Authenticate Docker with AWS ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build the Docker image
docker build -t $IMAGE_NAME:latest .

# Tag the Docker image
docker tag $IMAGE_NAME:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPOSITORY_NAME:latest

# Push the Docker image to ECR
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPOSITORY_NAME:latest
