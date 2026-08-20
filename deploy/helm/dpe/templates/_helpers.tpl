{{/*
Name helpers. Kubernetes names are capped at 63 characters, so every derived
name is truncated and stripped of a trailing dash.
*/}}
{{- define "dpe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dpe.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "dpe.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "dpe.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dpe.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels carried by every object the chart renders. */}}
{{- define "dpe.labels" -}}
helm.sh/chart: {{ include "dpe.chart" . }}
app.kubernetes.io/name: {{ include "dpe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Per-component selector labels. Selectors are immutable, so nothing
derived from values may appear here. */}}
{{- define "dpe.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dpe.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "dpe.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{- define "dpe.operatorServiceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-operator" (include "dpe.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "dpe.coordinatorName" -}}
{{- printf "%s-coordinator" (include "dpe.fullname" .) -}}
{{- end -}}

{{- define "dpe.redisName" -}}
{{- printf "%s-redis" (include "dpe.fullname" .) -}}
{{- end -}}

{{/*
Redis address used by the coordinator and by the sample ProcessingJob. Falls
back to redis.externalAddr when the bundled Redis is disabled, and fails the
render rather than installing something that cannot work.
*/}}
{{- define "dpe.redisAddr" -}}
{{- if .Values.redis.enabled -}}
{{- printf "%s:%d" (include "dpe.redisName" .) (int .Values.redis.port) -}}
{{- else if .Values.redis.externalAddr -}}
{{- .Values.redis.externalAddr -}}
{{- else -}}
{{- fail "redis.enabled is false, so redis.externalAddr must be set" -}}
{{- end -}}
{{- end -}}
