{{/*
Backward-compatibility shim for the global.* → global.seaweedfs.* migration.

When the chart is used as a subchart, .Values.global is shared with sibling
charts.  To avoid namespace pollution, app-specific values were moved under
global.seaweedfs.* (and global.registry was renamed to global.imageRegistry).

If a user still passes the OLD key paths (e.g. --set global.enableSecurity=true),
those keys will no longer have defaults in values.yaml, so their mere presence in
.Values.global means the user explicitly provided them.  This helper merges them
into global.seaweedfs.* so the rest of the templates see a single, canonical
location.

The helper mutates .Values.global.seaweedfs in-place via `set` and produces no
output.  It is idempotent (safe to call more than once in the same render).

Usage:  {{- include "seaweedfs.compat" . -}}
*/}}
{{- define "seaweedfs.compat" -}}
{{- $g  := .Values.global -}}
{{- $sw := $g.seaweedfs | default dict -}}

{{/* --- image-related renames --- */}}
{{- if hasKey $g "registry" -}}
{{-   $_ := set $g "imageRegistry" (default $g.imageRegistry $g.registry) -}}
{{- end -}}
{{- if hasKey $g "repository" -}}
{{-   $img := $sw.image | default dict -}}
{{-   $_ := set $img "repository" (default $img.repository $g.repository) -}}
{{-   $_ := set $sw "image" $img -}}
{{- end -}}
{{- if hasKey $g "imageName" -}}
{{-   $img := $sw.image | default dict -}}
{{-   $_ := set $img "name" (default $img.name $g.imageName) -}}
{{-   $_ := set $sw "image" $img -}}
{{- end -}}

{{/* --- scalar keys that moved 1:1 under global.seaweedfs --- */}}
{{- range $key := list "createClusterRole" "imagePullPolicy" "restartPolicy" "loggingLevel" "enableSecurity" "masterServer" "serviceAccountName" "automountServiceAccountToken" "enableReplication" "replicationPlacement" -}}
{{-   if hasKey $g $key -}}
{{-     $_ := set $sw $key (index $g $key) -}}
{{-   end -}}
{{- end -}}

{{/* --- nested dict keys: deep-merge so partial overrides work --- */}}
{{- range $key := list "securityConfig" "certificates" "monitoring" "serviceAccountAnnotations" "extraEnvironmentVars" -}}
{{-   if hasKey $g $key -}}
{{-     $old := index $g $key | default dict -}}
{{-     $new := index $sw $key | default dict -}}
{{-     if and (kindIs "map" $old) (kindIs "map" $new) -}}
{{-       $_ := set $sw $key (merge $old $new) -}}
{{-     else -}}
{{-       $_ := set $sw $key $old -}}
{{-     end -}}
{{-   end -}}
{{- end -}}

{{- $_ := set $g "seaweedfs" $sw -}}
{{- end -}}
