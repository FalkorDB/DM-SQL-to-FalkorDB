{{- define "dm-sql-to-falkordb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "dm-sql-to-falkordb.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.labels" -}}
helm.sh/chart: {{ include "dm-sql-to-falkordb.chart" . }}
app.kubernetes.io/name: {{ include "dm-sql-to-falkordb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "dm-sql-to-falkordb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dm-sql-to-falkordb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "dm-sql-to-falkordb.controlPlaneImage" -}}
{{- $global := .Values.global | default dict -}}
{{- $registry := default .Values.images.controlPlane.registry $global.imageRegistry -}}
{{- $repository := .Values.images.controlPlane.repository -}}
{{- $tag := .Values.images.controlPlane.tag | default .Values.images.defaultTag | default $global.version | default (printf "v%s" .Chart.AppVersion) -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.runnerImage" -}}
{{- $global := .Values.global | default dict -}}
{{- $registry := default .Values.images.runner.registry $global.imageRegistry -}}
{{- $repository := .Values.images.runner.repository -}}
{{- $tag := .Values.images.runner.tag | default .Values.images.defaultTag | default $global.version | default (printf "v%s" .Chart.AppVersion) -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.serviceAccountName" -}}
{{- if .Values.controlPlane.serviceAccount.create -}}
{{- default (include "dm-sql-to-falkordb.fullname" .) .Values.controlPlane.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.controlPlane.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.controlPlaneDataPvcName" -}}
{{- if .Values.persistence.existingClaim -}}
{{- .Values.persistence.existingClaim -}}
{{- else -}}
{{- printf "%s-data" (include "dm-sql-to-falkordb.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.runnerSharedPvcName" -}}
{{- if .Values.kubernetesRunner.sharedPvc.existingClaim -}}
{{- .Values.kubernetesRunner.sharedPvc.existingClaim -}}
{{- else -}}
{{- printf "%s-runner-shared" (include "dm-sql-to-falkordb.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.runnerNamespace" -}}
{{- default .Release.Namespace .Values.kubernetesRunner.namespaceOverride -}}
{{- end -}}

{{- define "dm-sql-to-falkordb.enabledToolsCsv" -}}
{{- $tools := list -}}
{{- range $tool, $enabled := .Values.tools.enabled -}}
{{- if $enabled -}}
{{- $tools = append $tools $tool -}}
{{- end -}}
{{- end -}}
{{- join "," (sortAlpha $tools) -}}
{{- end -}}
