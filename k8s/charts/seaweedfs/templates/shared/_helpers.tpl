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
Create a truncated component name.
Usage: {{ include "seaweedfs.componentName" (list . "component-suffix") }}
*/}}
{{- define "seaweedfs.componentName" -}}
{{- $context := index . 0 -}}
{{- $suffix := index . 1 -}}
{{- if gt (len $suffix) 61 -}}
{{-   fail (printf "Suffix '%s' is too long for componentName helper. Max length is 61." $suffix) -}}
{{- end -}}
{{- $fullname := include "seaweedfs.fullname" $context -}}
{{- $maxLen := sub 62 (len $suffix) | int -}}
{{- $truncatedFullname := trunc $maxLen $fullname | trimSuffix "-" -}}
{{- printf "%s-%s" $truncatedFullname $suffix -}}
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

{{- define "seaweedfs.mergeExtraEnvironmentVars" -}}
{{- $global := ((.global | default dict).extraEnvironmentVars | default dict) -}}
{{- $component := ((.component | default dict).extraEnvironmentVars | default dict) -}}
{{- $target := .target -}}
{{- range $key, $value := $global }}
{{- $_ := set $target $key $value }}
{{- end }}
{{- range $key, $value := $component }}
{{- $_ := set $target $key $value }}
{{- end }}
{{/* the license block owns SEAWEED_LICENSE; letting one through here too would
     render the key twice in one container */}}
{{- if ((.global | default dict).license | default dict).existingSecret }}
{{- $_ := unset $target "SEAWEED_LICENSE" }}
{{- end }}
{{- end -}}

{{/* Whether the mysql filer store is selected; a flag the chart cannot read counts as selected. */}}
{{- define "seaweedfs.filer.mysqlEnabled" -}}
{{- $merged := dict -}}
{{- $_ := include "seaweedfs.mergeExtraEnvironmentVars" (dict "global" .Values.global.seaweedfs "component" .Values.filer "target" $merged) -}}
{{- $enabled := index $merged "WEED_MYSQL_ENABLED" -}}
{{- if or (kindIs "map" $enabled) (hasKey (.Values.filer.secretExtraEnvironmentVars | default dict) "WEED_MYSQL_ENABLED") -}}
true
{{- else if and $enabled (eq (lower (toString $enabled)) "true") -}}
true
{{- end -}}
{{- end -}}

{{/* Return the proper filer image */}}
{{- define "seaweedfs.filer.image" -}}
{{- if .Values.filer.imageOverride -}}
{{- $imageOverride := .Values.filer.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper master image */}}
{{- define "seaweedfs.master.image" -}}
{{- if .Values.master.imageOverride -}}
{{- $imageOverride := .Values.master.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper s3 image */}}
{{- define "seaweedfs.s3.image" -}}
{{- if .Values.s3.imageOverride -}}
{{- $imageOverride := .Values.s3.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper sftp image */}}
{{- define "seaweedfs.sftp.image" -}}
{{- if .Values.sftp.imageOverride -}}
{{- $imageOverride := .Values.sftp.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper admin image */}}
{{- define "seaweedfs.admin.image" -}}
{{- if .Values.admin.imageOverride -}}
{{- $imageOverride := .Values.admin.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper worker image */}}
{{- define "seaweedfs.worker.image" -}}
{{- if .Values.worker.imageOverride -}}
{{- $imageOverride := .Values.worker.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Return the proper volume image */}}
{{- define "seaweedfs.volume.image" -}}
{{- if .Values.volume.imageOverride -}}
{{- $imageOverride := .Values.volume.imageOverride -}}
{{- printf "%s" $imageOverride -}}
{{- else -}}
{{- include "seaweedfs.image" . }}
{{- end -}}
{{- end -}}

{{/* Computes the container image name for all components (if they are not overridden) */}}
{{- define "seaweedfs.image" -}}
{{- $registryName := default .Values.image.registry .Values.global.imageRegistry | toString -}}
{{- $repositoryName := default .Values.image.repository .Values.global.seaweedfs.image.repository | toString -}}
{{- $name := .Values.global.seaweedfs.image.name | toString -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag  | toString -}}
{{- if .Values.image.repository -}}
{{-   $name = $repositoryName -}}
{{- else if $repositoryName -}}
{{-   $name = printf "%s/%s" (trimSuffix "/" $repositoryName) (base $name) -}}
{{- end -}}
{{- if $registryName -}}
{{-   printf "%s/%s:%s" $registryName $name $tag -}}
{{- else -}}
{{-   printf "%s:%s" $name $tag -}}
{{- end -}}
{{- end -}}

{{/* check if any Volume PVC exists */}}
{{- define "seaweedfs.volume.pvc_exists" -}}
{{- if or (or (eq .Values.volume.data.type "persistentVolumeClaim") (and (eq .Values.volume.idx.type "persistentVolumeClaim") .Values.volume.dir_idx )) (eq .Values.volume.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any Filer PVC exists */}}
{{- define "seaweedfs.filer.pvc_exists" -}}
{{- if or (eq .Values.filer.data.type "persistentVolumeClaim") (eq .Values.filer.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any Master PVC exists */}}
{{- define "seaweedfs.master.pvc_exists" -}}
{{- if or (eq .Values.master.data.type "persistentVolumeClaim") (eq .Values.master.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any Admin PVC exists */}}
{{- define "seaweedfs.admin.pvc_exists" -}}
{{- if or (eq .Values.admin.data.type "persistentVolumeClaim") (eq .Values.admin.logs.type "persistentVolumeClaim") -}}
{{- printf "true" -}}
{{- else -}}
{{- printf "" -}}
{{- end -}}
{{- end -}}

{{/* check if any InitContainers exist for Volumes */}}
{{- define "seaweedfs.volume.initContainers_exists" -}}
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
{{ include "seaweedfs.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ ) }}
{{ include "seaweedfs.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ "scope" $app ) }}
*/}}
{{- define "seaweedfs.tplvalues.render" -}}
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
{{ include "seaweedfs.resource-quantity" "10Gi" }}
*/}}
{{- define "seaweedfs.resource-quantity" -}}
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
{{- define "seaweedfs.getOrGeneratePassword" -}}
{{- $params := . -}}
{{- $namespace := $params.namespace -}}
{{- $secretName := $params.secretName -}}
{{- $key := $params.key -}}
{{- $length := default 16 $params.length -}}

{{- $existingSecret := default (lookup "v1" "Secret" $namespace $secretName) $params.existingSecret -}}
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
{{- $component := ternary "all-in-one" "master" .Values.allInOne.enabled -}}
{{- printf "%s.%s:%d" (include "seaweedfs.componentName" (list . $component)) .Release.Namespace (int .Values.master.port) -}}
{{- end -}}

{{/*
Compute the filer service address to be used in cluster env vars.
If allInOne is enabled, point to the all-in-one service; otherwise, point to the filer-client service.
*/}}
{{- define "seaweedfs.cluster.filerAddress" -}}
{{- $component := ternary "all-in-one" "filer-client" .Values.allInOne.enabled -}}
{{- printf "%s.%s:%d" (include "seaweedfs.componentName" (list . $component)) .Release.Namespace (int .Values.filer.port) -}}
{{- end -}}

{{/*
Generate comma-separated list of master server addresses.
Usage: {{ include "seaweedfs.masterServers" . }}
Output example: my-release-master-0.my-release-master.namespace:9333,my-release-master-1...
*/}}
{{- define "seaweedfs.masterServers" -}}
{{- $masterName := include "seaweedfs.componentName" (list . "master") -}}
{{- range $index := until (.Values.master.replicas | int) -}}
{{- if $index }},{{ end -}}
{{ $masterName }}-{{ $index }}.{{ $masterName }}.{{ $.Release.Namespace }}:{{ $.Values.master.port }}
{{- end -}}
{{- end -}}

{{/*
Generate master server argument value, using global.masterServer if set, otherwise the generated list.
Usage: {{ include "seaweedfs.masterServerArg" . }}
*/}}
{{- define "seaweedfs.masterServerArg" -}}
{{- if .Values.global.seaweedfs.masterServer -}}
{{- .Values.global.seaweedfs.masterServer -}}
{{- else -}}
{{- include "seaweedfs.masterServers" . -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "seaweedfs.serviceAccountName" -}}
{{- .Values.global.seaweedfs.serviceAccountName | default "seaweedfs" -}}
{{- end -}}

{{/* True when security.toml should be rendered and mounted. volumeWrite is
     excluded unless its non-default expiration is configured. */}}
{{- define "seaweedfs.securityConfigEnabled" -}}
{{- $sec := (.Values.global.seaweedfs).securityConfig | default dict -}}
{{- $jwt := $sec.jwtSigning | default dict -}}
{{- $expiresAfterSeconds := $jwt.expiresAfterSeconds | default dict -}}
{{- $volumeWriteExpirationConfigured := and $jwt.volumeWrite (gt (int $expiresAfterSeconds.volumeWrite) 0) -}}
{{- if or .Values.global.seaweedfs.enableSecurity $volumeWriteExpirationConfigured $jwt.volumeRead $jwt.filerWrite $jwt.filerRead -}}
true
{{- end -}}
{{- end -}}

{{/* True when the post-install bucket hook Job renders: an S3 endpoint, plus
     buckets to create on it. Read by the Job itself and by its NetworkPolicy,
     which has to appear exactly when the Job does - a Job without its policy
     hangs in a default-deny namespace. */}}
{{- define "seaweedfs.bucketHookEnabled" -}}
{{- if .Values.allInOne.enabled -}}
{{-   if and .Values.allInOne.s3.enabled .Values.allInOne.s3.createBuckets -}}
true
{{-   end -}}
{{- else if .Values.master.enabled -}}
{{-   if and (or .Values.filer.s3.enabled .Values.s3.enabled) (or .Values.s3.createBuckets .Values.filer.s3.createBuckets) -}}
true
{{-   end -}}
{{- end -}}
{{- end -}}

{{/* The kubectl commands the volume resize hook has to run, one per line: a
     cascade-orphan delete for every StatefulSet whose volumeClaimTemplates no
     longer match the values, and a patch for every PVC the values grew.

     Empty when there is nothing to resize, which is what gates the Job. Read by
     its NetworkPolicy too, which has to appear exactly when the Job does - a Job
     without its policy hangs in a default-deny namespace, and a policy without
     its Job is an orphaned hook resource on every install.

     Built on lookup, so it is always empty under helm template and on a fresh
     install, where there is no StatefulSet to compare against yet. */}}
{{- define "seaweedfs.volumeResizeHookCommands" -}}
{{- $seaweedfsName := include "seaweedfs.fullname" $ }}
{{- $volumes := deepCopy .Values.volumes | mergeOverwrite (dict "" .Values.volume) }}
{{- $commands := list }}
{{- if .Values.volume.resizeHook.enabled }}
{{-   range $vname, $volume := $volumes }}
{{-     $volumeName := trimSuffix "-" (printf "volume-%s" $vname) }}
{{-     $volume := mergeOverwrite (deepCopy $.Values.volume) (dict "enabled" true) $volume }}
{{-     if $volume.enabled }}
{{-       $replicas := int $volume.replicas }}
{{-       $statefulsetName := printf "%s-%s" $seaweedfsName $volumeName }}
{{-       $statefulset := (lookup "apps/v1" "StatefulSet" $.Release.Namespace $statefulsetName) }}
{{- /* Check for changes in volumeClaimTemplates */}}
{{-       if $statefulset }}
{{-         range $dir := $volume.dataDirs }}
{{-           if eq .type "persistentVolumeClaim" }}
{{-             $desiredSize := .size }}
{{-             range $statefulset.spec.volumeClaimTemplates }}
{{-               if and (eq .metadata.name $dir.name) (ne .spec.resources.requests.storage $desiredSize) }}
{{-                 $commands = append $commands (printf "kubectl delete statefulset %s --cascade=orphan" $statefulsetName) }}
{{-               end }}
{{-             end }}
{{-           end }}
{{-         end }}
{{-       end }}
{{- /* Check for the need for patching existing PVCs */}}
{{-       range $dir := $volume.dataDirs }}
{{-         if eq .type "persistentVolumeClaim" }}
{{-           $desiredSize := .size }}
{{-           range $i, $e := until $replicas }}
{{-             $pvcName := printf "%s-%s-%s-%d" $dir.name $seaweedfsName $volumeName $e }}
{{-             $currentPVC := (lookup "v1" "PersistentVolumeClaim" $.Release.Namespace $pvcName) }}
{{-             if $currentPVC }}
{{-               $oldSize := include "seaweedfs.resource-quantity" $currentPVC.spec.resources.requests.storage }}
{{-               $newSize := include "seaweedfs.resource-quantity" $desiredSize }}
{{-               if gt $newSize $oldSize }}
{{-                 $commands = append $commands (printf "kubectl patch pvc %s-%s-%s-%d -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"%s\"}}}}'" $dir.name $seaweedfsName $volumeName $e $desiredSize) }}
{{-               end }}
{{-             end }}
{{-           end }}
{{-         end }}
{{-       end }}
{{-     end }}
{{-   end }}
{{- end }}
{{- join "\n" $commands }}
{{- end -}}

{{/* S3 TLS cert/key arguments, using custom secret if s3.tlsSecret is set */}}
{{- define "seaweedfs.s3.tlsArgs" -}}
{{- $prefix := .prefix -}}
{{- $root := .root -}}
{{- if $root.Values.s3.tlsSecret -}}
-{{ $prefix }}cert.file=/usr/local/share/ca-certificates/s3/tls.crt \
-{{ $prefix }}key.file=/usr/local/share/ca-certificates/s3/tls.key \
{{- else -}}
-{{ $prefix }}cert.file=/usr/local/share/ca-certificates/client/tls.crt \
-{{ $prefix }}key.file=/usr/local/share/ca-certificates/client/tls.key \
{{- end -}}
{{- end -}}

{{/* S3 custom TLS volume mount */}}
{{- define "seaweedfs.s3.tlsVolumeMount" -}}
{{- if .Values.s3.tlsSecret }}
- name: s3-tls-cert
  readOnly: true
  mountPath: /usr/local/share/ca-certificates/s3/
{{- end }}
{{- end -}}

{{/* S3 custom TLS volume */}}
{{- define "seaweedfs.s3.tlsVolume" -}}
{{- if .Values.s3.tlsSecret }}
- name: s3-tls-cert
  secret:
    secretName: {{ .Values.s3.tlsSecret }}
{{- end }}
{{- end -}}

{{/* True when an enterprise license Secret is configured. */}}
{{- define "seaweedfs.licenseEnabled" -}}
{{- if ((.Values.global.seaweedfs).license).existingSecret -}}
true
{{- end -}}
{{- end -}}

{{/* Enterprise license volume. Projects just the license key. */}}
{{- define "seaweedfs.licenseVolume" -}}
{{- if include "seaweedfs.licenseEnabled" . -}}
- name: seaweedfs-license
  secret:
    secretName: {{ .Values.global.seaweedfs.license.existingSecret }}
    defaultMode: 0444
    items:
      - key: {{ .Values.global.seaweedfs.license.secretKey | default "seaweed-license.json" | quote }}
        path: {{ .Values.global.seaweedfs.license.secretKey | default "seaweed-license.json" | quote }}
{{- end }}
{{- end -}}

{{/* Enterprise license volume mount. Never a subPath: that is resolved once at
     container start, so a renewed Secret would not reach a running master. */}}
{{- define "seaweedfs.licenseVolumeMount" -}}
{{- if include "seaweedfs.licenseEnabled" . -}}
- name: seaweedfs-license
  readOnly: true
  mountPath: {{ .Values.global.seaweedfs.license.mountPath | default "/etc/seaweedfs/license" | quote }}
{{- end }}
{{- end -}}

{{/* SEAWEED_LICENSE, set explicitly rather than relying on the binary's search
     paths, which depend on the working directory. */}}
{{- define "seaweedfs.licenseEnv" -}}
{{- if include "seaweedfs.licenseEnabled" . -}}
- name: SEAWEED_LICENSE
  value: {{ printf "%s/%s"
      (.Values.global.seaweedfs.license.mountPath | default "/etc/seaweedfs/license")
      (.Values.global.seaweedfs.license.secretKey | default "seaweed-license.json") | quote }}
{{- end }}
{{- end -}}

{{/* Name of the environment variable carrying one generated S3 credential
     field, e.g. SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID. The generated identities file
     names it in place of the key when the key lives in an existing Secret.
     Usage: include "seaweedfs.s3.credentialEnvName" (list "admin" "accessKey") */}}
{{- define "seaweedfs.s3.credentialEnvName" -}}
{{- $identity := index . 0 -}}
{{- $field := index . 1 -}}
{{- printf "SEAWEEDFS_S3_%s_%s" (upper $identity) (ternary "ACCESS_KEY_ID" "SECRET_ACCESS_KEY" (eq $field "accessKey")) -}}
{{- end -}}

{{/* Environment for the S3 identities the chart generates from an existing
     Secret. The gateway resolves the ${VAR} references the identities file
     carries, so the keys never enter the rendered manifests and a dry run
     renders the same as an install. */}}
{{- define "seaweedfs.s3.credentialEnv" -}}
{{- $creds := $.Values.s3.credentials | default dict -}}
{{- range $identity := list "admin" "read" -}}
{{- $identityCreds := index $creds $identity | default dict -}}
{{- if $identityCreds.existingSecret }}
- name: {{ include "seaweedfs.s3.credentialEnvName" (list $identity "accessKey") }}
  valueFrom:
    secretKeyRef:
      name: {{ $identityCreds.existingSecret | quote }}
      key: {{ default (printf "%s_access_key_id" $identity) $identityCreds.accessKeyKey | quote }}
- name: {{ include "seaweedfs.s3.credentialEnvName" (list $identity "secretKey") }}
  valueFrom:
    secretKeyRef:
      name: {{ $identityCreds.existingSecret | quote }}
      key: {{ default (printf "%s_secret_access_key" $identity) $identityCreds.secretKeyKey | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Generate a compatible trafficDistribution value due to "PreferClose" fast deprecation in k8s v1.35.
     Accepts a dict with "value" (the trafficDistribution string) and "Capabilities". */}}
{{- define "seaweedfs.trafficDistribution" -}}
{{- if .value -}}
{{- and (eq .value "PreferClose") (semverCompare ">=1.35-0" .Capabilities.KubeVersion.GitVersion) | ternary "PreferSameZone" .value -}}
{{- end -}}
{{- end -}}
