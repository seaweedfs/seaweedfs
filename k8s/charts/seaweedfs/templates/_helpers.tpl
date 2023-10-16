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
{{- $registryName := .Values.image.registry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := default .Values.filer.imageName .Values.global.imageName | toString -}}
{{- $tag := coalesce .Values.filer.imageTag .Values.global.imageTag .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper master image */}}
{{- define "master.image" -}}
{{- if .Values.master.imageOverride -}}
{{- $imageOverride := .Values.master.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := .Values.image.registry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := default .Values.master.imageName .Values.global.imageName | toString -}}
{{- $tag := coalesce .Values.master.imageTag .Values.global.imageTag .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper s3 image */}}
{{- define "s3.image" -}}
{{- if .Values.s3.imageOverride -}}
{{- $imageOverride := .Values.s3.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := .Values.image.registry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := default .Values.s3.imageName .Values.global.imageName | toString -}}
{{- $tag := coalesce .Values.s3.imageTag .Values.global.imageTag .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* Return the proper volume image */}}
{{- define "volume.image" -}}
{{- if .Values.volume.imageOverride -}}
{{- $imageOverride := .Values.volume.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- $registryName := .Values.image.registry | toString -}}
{{- $repositoryName := .Values.image.repository | toString -}}
{{- $name := default .Values.volume.imageName .Values.global.imageName | toString -}}
{{- $tag := coalesce .Values.volume.imageTag .Values.global.imageTag .Chart.AppVersion | toString -}}
{{- printf "%s%s%s:%s" $registryName $repositoryName $name $tag -}}
{{- end -}}
{{- end -}}

{{/* check if any Volume PVC exists */}}
{{- define "volume.pvc_exists" -}}
{{- if or (or (eq .Values.volume.data.type "persistentVolumeClaim") (and (eq .Values.volume.idx.type "persistentVolumeClaim") .Values.volume.dir_idx )) (eq .Values.volume.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* check if any Volume HostPath exists */}}
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

{{/* check if any Filer PVC exists */}}
{{- define "filer.pvc_exists" -}}
{{- if or (eq .Values.filer.data.type "persistentVolumeClaim") (eq .Values.filer.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* check if any Filer HostPath exists */}}
{{- define "filer.hostpath_exists" -}}
{{- if or (eq .Values.filer.data.type "hostPath") (eq .Values.filer.logs.type "hostPath") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* check if any Master PVC exists */}}
{{- define "master.pvc_exists" -}}
{{- if or (eq .Values.master.data.type "persistentVolumeClaim") (eq .Values.master.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* check if any Master HostPath exists */}}
{{- define "master.hostpath_exists" -}}
{{- if or (eq .Values.master.data.type "hostPath") (eq .Values.master.logs.type "hostPath") -}}
{{- printf "true" -}}
{{- else -}}
{{- if or .Values.global.enableSecurity .Values.volume.extraVolumes -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* check if any InitContainers exist for Volumes */}}
{{- define "volume.initContainers_exists" -}}
{{- if or (not (empty .Values.volume.dir_idx )) (not (empty .Values.volume.initContainers )) -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "false" -}}
{{- end -}}
{{- end -}}

{{/* Return the proper imagePullSecrets */}}
{{- define "seaweedfs.imagePullSecrets" -}}
{{- if .Values.global.imagePullSecrets }}
{{- if kindIs "string" .Values.global.imagePullSecrets }}
imagePullSecrets:
  - name: {{ .Values.global.imagePullSecrets }}
{{- else }}
imagePullSecrets:
{{- range .Values.global.imagePullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
