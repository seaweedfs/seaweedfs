{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to
this (by the DNS naming spec). If release name contains chart name it will
be used as a full name.
*/}}
{{- define "seaweedfs.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "seaweedfs.chart" -}}
{{- printf "%s-helm" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "seaweedfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Inject extra environment vars in the format key:value, if populated
*/}}
{{- define "seaweedfs.extraEnvironmentVars" -}}
{{- if .extraEnvironmentVars -}}
{{- range $key, $value := .extraEnvironmentVars }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Return the proper filer image */}}
{{- define "filer.image" -}}
{{- if .Values.filer.imageOverride -}}
{{- $imageOverride := .Values.filer.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.image.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper dbSchema image */}}
{{- define "filer.dbSchema.image" -}}
{{- if .Values.filer.dbSchema.imageOverride -}}
{{- $imageOverride := .Values.filer.dbSchema.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.global.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.global.repository | toString -}}
{{- $name := .Values.filer.dbSchema.imageName | toString -}}
{{- $tag := .Values.filer.dbSchema.imageTag | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper master image */}}
{{- define "master.image" -}}
{{- if .Values.master.imageOverride -}}
{{- $imageOverride := .Values.master.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.image.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper s3 image */}}
{{- define "s3.image" -}}
{{- if .Values.s3.imageOverride -}}
{{- $imageOverride := .Values.s3.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.image.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper volume image */}}
{{- define "volume.image" -}}
{{- if .Values.volume.imageOverride -}}
{{- $imageOverride := .Values.volume.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.image.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper cronjob image */}}
{{- define "cronjob.image" -}}
{{- if .Values.cronjob.imageOverride -}}
{{- $imageOverride := .Values.cronjob.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := default .Values.image.registry .Values.global.localRegistry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}


{{/* check if any PVC exists */}}
{{- define "volume.pvc_exists" -}}
{{- if or (or (eq .Values.volume.data.type "persistentVolumeClaim") (and (eq .Values.volume.idx.type "persistentVolumeClaim") .Values.volume.dir_idx )) (eq .Values.volume.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* check if any HostPath exists */}}
{{- define "volume.hostpath_exists" -}}
{{- if or (or (eq .Values.volume.data.type "hostPath") (and (eq .Values.volume.idx.type "hostPath") .Values.volume.dir_idx )) (eq .Values.volume.logs.type "hostPath") -}}
{{- printf "true" -}}
{{- else -}}
{{- if or .Values.global.enableSecurity .Values.volume.extraVolumes -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}
{{- end -}}
