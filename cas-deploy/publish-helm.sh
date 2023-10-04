#!/bin/bash

path_to_chart="../k8s/charts/seaweedfs"
repo_name="cas-external-chart-seaweedfs"
chart_name="seaweedfs"
profile="$AWS_PROFILE"
region="$AWS_REGION"
account_id="$AWS_ACCOUNT_ID"
version=$(grep -E 'version' $path_to_chart/Chart.yaml | awk '{print $NF}' | sed 's/://')

sed -i.bak "s/^name: .*/name: $repo_name/" "$path_to_chart/Chart.yaml" && rm -rf "$path_to_chart/chart.yaml.bak"

# mv "$chart_name" "$repo_name"

helm package $path_to_chart

if aws ecr describe-repositories --region $region --profile $profile --repository-names $repo_name > /dev/null 2>&1; then
    echo "ECR repository '$repo_name' already exists."
else
aws ecr create-repository \
     --repository-name $repo_name \
     --region $region \
     --profile $profile > /dev/null 2>&1
fi

aws ecr get-login-password \
     --profile $profile --region $region | helm registry login \
     --username AWS \
     --password-stdin $account_id.dkr.ecr.$region.amazonaws.com

helm push $repo_name-$version.tgz oci://$account_id.dkr.ecr.$region.amazonaws.com/
rm -rf $repo_name-$version.tgz
sed -i.bak "s/^name: .*/name: seaweedfs/" "$path_to_chart/Chart.yaml" && rm -rf "$path_to_chart/chart.yaml.bak"