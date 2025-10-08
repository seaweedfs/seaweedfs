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
{{- include "common.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper master image */}}
{{- define "master.image" -}}
{{- if .Values.master.imageOverride -}}
{{- $imageOverride := .Values.master.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "common.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper s3 image */}}
{{- define "s3.image" -}}
{{- if .Values.s3.imageOverride -}}
{{- $imageOverride := .Values.s3.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "common.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper sftp image */}}
{{- define "sftp.image" -}}
{{- if .Values.sftp.imageOverride -}}
{{- $imageOverride := .Values.sftp.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "common.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper volume image */}}
{{- define "volume.image" -}}
{{- if .Values.volume.imageOverride -}}
{{- $imageOverride := .Values.volume.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "common.image" . }}
{{- end -}}
{{- end -}}

{{/* Computes the container image name for all components (if they are not overridden) */}}
{{- define "common.image" -}}
{{- $registryName := default .Values.image.registry .Values.global.registry | toString -}}
{{- $repositoryName := default .Values.image.repository .Values.global.repository | toString -}}
{{- $name := .Values.global.imageName | toString -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag  | toString -}}
{{- if $repositoryName -}}
{{-   $name = printf "%s/%s" (trimSuffix "/" $repositoryName) (base $name) -}}
{{- end -}}
{{- if $registryName -}}
{{-   printf "%s/%s:%s" $registryName $name $tag -}}
{{- else -}}
{{-   printf "%s:%s" $name $tag -}}
{{- end -}}
{{- end -}}

{{/* check if any Volume PVC exists */}}
{{- define "volume.pvc_exists" -}}
{{- if or (or (eq .Values.volume.data.type "persistentVolumeClaim") (and (eq .Values.volume.idx.type "persistentVolumeClaim") .Values.volume.dir_idx )) (eq .Values.volume.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any Filer PVC exists */}}
{{- define "filer.pvc_exists" -}}
{{- if or (eq .Values.filer.data.type "persistentVolumeClaim") (eq .Values.filer.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any Master PVC exists */}}
{{- define "master.pvc_exists" -}}
{{- if or (eq .Values.master.data.type "persistentVolumeClaim") (eq .Values.master.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any InitContainers exist for Volumes */}}
{{- define "volume.initContainers_exists" -}}
{{- if or (not (empty .Values.volume.idx )) (not (empty .Values.volume.initContainers )) -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* Return the proper imagePullSecrets */}}
{{- define "seaweedfs.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- if kindIs "string" . }}
  - name: {{ . }}
{{- else }}
{{- range . }}
  {{- if kindIs "string" . }}
  - name: {{ . }}
  {{- else }}
  - {{ toYaml . }}
  {{- end}}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Renders a value that contains template perhaps with scope if the scope is present.
Usage:
{{ include "common.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ ) }}
{{ include "common.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ "scope" $app ) }}
*/}}
{{- define "common.tplvalues.render" -}}
{{- $value := typeIs "string" .value | ternary .value (.value | toYaml) }}
{{- if contains "{{" (toJson .value) }}
  {{- if .scope }}
      {{- tpl (cat "{{- with $.RelativeScope -}}" $value "{{- end }}") (merge (dict "RelativeScope" .scope) .context) }}
  {{- else }}
    {{- tpl $value .context }}
  {{- end }}
{{- else }}
    {{- $value }}
{{- end }}
{{- end -}}

{{/*
Converts a Kubernetes quantity like "256Mi" or "2G" to a float64 in base units,
handling both binary (Ki, Mi, Gi) and decimal (m, k, M) suffixes; numeric inputs
Usage:
{{ include "common.resource-quantity" "10Gi" }}
*/}}
{{- define "common.resource-quantity" -}}
    {{- $value := . -}}
    {{- $unit := 1.0 -}}
    {{- if typeIs "string" . -}}
        {{- $base2 := dict "Ki" 0x1p10 "Mi" 0x1p20 "Gi" 0x1p30 "Ti" 0x1p40 "Pi" 0x1p50 "Ei" 0x1p60 -}}
        {{- $base10 := dict "m" 1e-3 "k" 1e3 "M" 1e6 "G" 1e9 "T" 1e12 "P" 1e15 "E" 1e18 -}}
        {{- range $k, $v := merge $base2 $base10 -}}
            {{- if hasSuffix $k $ -}}
                {{- $value = trimSuffix $k $ -}}
                {{- $unit = $v -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- mulf (float64 $value) $unit -}}
{{- end -}}

{{/*
getOrGeneratePassword will check if a password exists in a secret and return it,
or generate a new random password if it doesn't exist.
*/}}
{{- define "getOrGeneratePassword" -}}
{{- $params := . -}}
{{- $namespace := $params.namespace -}}
{{- $secretName := $params.secretName -}}
{{- $key := $params.key -}}
{{- $length := default 16 $params.length -}}

{{- $existingSecret := lookup "v1" "Secret" $namespace $secretName -}}
{{- if and $existingSecret (index $existingSecret.data $key) -}}
  {{- index $existingSecret.data $key | b64dec -}}
{{- else -}}
  {{- randAlphaNum $length -}}
{{- end -}}
{{- end -}}

{{/*
Compute the master service address to be used in cluster env vars.
If allInOne is enabled, point to the all-in-one service; otherwise, point to the master service.
*/}}
{{- define "seaweedfs.cluster.masterAddress" -}}
{{- $serviceNameSuffix := "-master" -}}
{{- if .Values.allInOne.enabled -}}
{{-   $serviceNameSuffix = "-all-in-one" -}}
{{- end -}}
{{- printf "%s%s.%s:%d" (include "seaweedfs.name" .) $serviceNameSuffix .Release.Namespace (int .Values.master.port) -}}
{{- end -}}

{{/*
Compute the filer service address to be used in cluster env vars.
If allInOne is enabled, point to the all-in-one service; otherwise, point to the filer-client service.
*/}}
{{- define "seaweedfs.cluster.filerAddress" -}}
{{- $serviceNameSuffix := "-filer-client" -}}
{{- if .Values.allInOne.enabled -}}
{{-   $serviceNameSuffix = "-all-in-one" -}}
{{- end -}}
{{- printf "%s%s.%s:%d" (include "seaweedfs.name" .) $serviceNameSuffix .Release.Namespace (int .Values.filer.port) -}}
{{- end -}}
