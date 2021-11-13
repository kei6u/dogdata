{{/*
Expand the name of the chart.
*/}}
{{- define "dogdata.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dogdata.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dogdata.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dogdata.labels" -}}
helm.sh/chart: {{ include "dogdata.chart" . }}
{{ include "dogdata.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dogdata.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dogdata.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "dogdata.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dogdata.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create a default fuuly qualified postgres name.
*/}}
{{- define "dogdataPostgres.fullname" -}}
{{- printf "%s-postgres" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels for postgres
*/}}
{{- define "dogdataPostgres.labels" -}}
helm.sh/chart: {{ include "dogdata.chart" . }}
{{ include "dogdataPostgres.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for postgres
*/}}
{{- define "dogdataPostgres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dogdata.name" . }}-postgres
app.kubernetes.io/instance: {{ .Release.Name }}-postgres
{{- end }}
