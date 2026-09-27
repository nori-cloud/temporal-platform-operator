{{/* Expand the chart name. */}}
{{- define "temporal-platform-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Create a release-scoped name. */}}
{{- define "temporal-platform-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Common labels. */}}
{{- define "temporal-platform-operator.labels" -}}
helm.sh/chart: {{ include "temporal-platform-operator.chart" . }}
{{ include "temporal-platform-operator.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels. */}}
{{- define "temporal-platform-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "temporal-platform-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Chart name and version. */}}
{{- define "temporal-platform-operator.chart" -}}
{{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end }}

{{/* The configured installation namespace. */}}
{{- define "temporal-platform-operator.namespace" -}}
{{- default .Release.Namespace .Values.namespace -}}
{{- end }}

{{/* Operator service account name. */}}
{{- define "temporal-platform-operator.serviceAccountName" -}}
{{ include "temporal-platform-operator.fullname" . }}
{{- end }}
