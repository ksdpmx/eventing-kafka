/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/health"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logtesting "knative.dev/pkg/logging/testing"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

type deploymentOption func(service *appsv1.Deployment)
type serviceOption func(*corev1.Service)

// Tests the CheckDeploymentChanged functionality.  Note that this is also tested fairly extensively
// as part of the various reconciler tests, and as such the deployment structs used here are somewhat trivial.
func TestCheckDeploymentChanged(t *testing.T) {
	defaultMode := int32(420)
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name               string
		existingDeployment *appsv1.Deployment
		newDeployment      *appsv1.Deployment
		expectUpdated      bool
	}{
		{
			name:               "Different Image",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withDifferentImage),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Deployment Label",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withDeploymentLabel),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Template Label",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withTemplateLabel),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Deployment Annotation",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withDeploymentAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Template Annotation",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withTemplateAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Missing Existing Container",
			existingDeployment: getBasicDeployment(withoutContainer),
			newDeployment:      getBasicDeployment(),
			expectUpdated:      true,
		},
		{
			name:               "Extra Existing Deployment Label",
			existingDeployment: getBasicDeployment(withDeploymentLabel),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Extra Existing Template Label",
			existingDeployment: getBasicDeployment(withTemplateLabel),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Extra Existing Deployment Annotation",
			existingDeployment: getBasicDeployment(withDeploymentAnnotation),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Extra Existing Template Annotation",
			existingDeployment: getBasicDeployment(withTemplateAnnotation),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Missing New Container",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withoutContainer),
		},
		{
			name:               "Multiple Existing Containers",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Multiple Existing Containers, Incorrect First",
			existingDeployment: getBasicDeployment(withExtraContainerFirst),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Multiple Existing Containers, Missing Required Deployment Annotation",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(withDeploymentAnnotation),
			expectUpdated:      true,
		}, {
			name:               "Multiple Existing Containers, Missing Required Template Annotation",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(withTemplateAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Multiple Existing Containers, Incorrect First, Missing Required Deployment Annotation",
			existingDeployment: getBasicDeployment(withExtraContainerFirst),
			newDeployment:      getBasicDeployment(withDeploymentAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Multiple Existing Containers, Incorrect First, Missing Required Template Annotation",
			existingDeployment: getBasicDeployment(withExtraContainerFirst),
			newDeployment:      getBasicDeployment(withTemplateAnnotation),
			expectUpdated:      true,
		},
		{
			name:               "Container With Incorrect Name",
			existingDeployment: getBasicDeployment(withDifferentContainer),
			newDeployment:      getBasicDeployment(),
			expectUpdated:      true,
		},
		{
			name: "Test with more fields",
			existingDeployment: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       constants.DeploymentKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-cluster-ttt-32603413-receiver",
					Namespace: "knative-eventing",
					Labels: map[string]string{
						constants.AppLabel:                  "kafka-cluster-ttt-32603413-receiver",
						constants.KafkaChannelReceiverLabel: "true",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							constants.AppLabel: "kafka-cluster-ttt-32603413-receiver",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								constants.AppLabel:                  "kafka-cluster-ttt-32603413-receiver",
								constants.KafkaChannelReceiverLabel: "true",
							},
						},
						Spec: corev1.PodSpec{
							ServiceAccountName: "kafka-cluster-ttt-32603413-receiver",
							Containers: []corev1.Container{
								{
									Name: "receiver",
									LivenessProbe: &corev1.Probe{
										Handler: corev1.Handler{
											HTTPGet: &corev1.HTTPGetAction{
												Port: intstr.FromInt(constants.HealthPort),
												Path: health.LivenessPath,
											},
										},
										InitialDelaySeconds: constants.ChannelLivenessDelay,
										PeriodSeconds:       constants.ChannelLivenessPeriod,
										TimeoutSeconds:      constants.ChannelLivenessTimeout,
										SuccessThreshold:    constants.ChannelLivenessSuccessThreshold,
										FailureThreshold:    constants.ChannelLivenessFailureThreshold,
									},
									ReadinessProbe: &corev1.Probe{
										Handler: corev1.Handler{
											HTTPGet: &corev1.HTTPGetAction{
												Port: intstr.FromInt(constants.HealthPort),
												Path: health.ReadinessPath,
											},
										},
										InitialDelaySeconds: constants.ChannelReadinessDelay,
										PeriodSeconds:       constants.ChannelReadinessPeriod,
										TimeoutSeconds:      constants.ChannelReadinessTimeout,
										SuccessThreshold:    constants.ChannelReadinessSuccessThreshold,
										FailureThreshold:    constants.ChannelReadinessFailureThreshold,
									},
									Image: "gcr.io",
									Ports: []corev1.ContainerPort{
										{
											Name:          "server",
											ContainerPort: int32(constants.HttpContainerPortNumber),
										},
									},
									Env:             []corev1.EnvVar{},
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.Quantity{
												Format: "300m",
											},
											corev1.ResourceMemory: resource.Quantity{
												Format: "256Mi",
											},
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.Quantity{
												Format: "1",
											},
											corev1.ResourceMemory: resource.Quantity{
												Format: "3Gi",
											},
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      commonconstants.SettingsConfigMapName,
											MountPath: commonconstants.SettingsConfigMapMountPath,
										},
									},
								},
							},
							NodeSelector: map[string]string{},
							Affinity:     &corev1.Affinity{},
							Volumes: []corev1.Volume{
								{
									Name: commonconstants.SettingsConfigMapName,
									VolumeSource: corev1.VolumeSource{
										ConfigMap: &corev1.ConfigMapVolumeSource{
											DefaultMode: &defaultMode,
											LocalObjectReference: corev1.LocalObjectReference{
												Name: commonconstants.SettingsConfigMapName + "-default",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			newDeployment: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       constants.DeploymentKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-cluster-ttt-32603413-receiver",
					Namespace: "knative-eventing",
					Labels: map[string]string{
						constants.AppLabel:                  "kafka-cluster-ttt-32603413-receiver",
						constants.KafkaChannelReceiverLabel: "true",
					},
				},
				Spec: appsv1.DeploymentSpec{
					// Replicas: 8,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							constants.AppLabel: "kafka-cluster-ttt-32603413-receiver",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								constants.AppLabel:                  "kafka-cluster-ttt-32603413-receiver",
								constants.KafkaChannelReceiverLabel: "true",
							},
						},
						Spec: corev1.PodSpec{
							ServiceAccountName: "kafka-cluster-ttt-32603413-receiver",
							Containers: []corev1.Container{
								{
									Name: "receiver",
									LivenessProbe: &corev1.Probe{
										Handler: corev1.Handler{
											HTTPGet: &corev1.HTTPGetAction{
												Port: intstr.FromInt(constants.HealthPort),
												Path: health.LivenessPath,
											},
										},
										InitialDelaySeconds: constants.ChannelLivenessDelay,
										PeriodSeconds:       constants.ChannelLivenessPeriod,
										TimeoutSeconds:      constants.ChannelLivenessTimeout,
										SuccessThreshold:    constants.ChannelLivenessSuccessThreshold,
										FailureThreshold:    constants.ChannelLivenessFailureThreshold,
									},
									ReadinessProbe: &corev1.Probe{
										Handler: corev1.Handler{
											HTTPGet: &corev1.HTTPGetAction{
												Port: intstr.FromInt(constants.HealthPort),
												Path: health.ReadinessPath,
											},
										},
										InitialDelaySeconds: constants.ChannelReadinessDelay,
										PeriodSeconds:       constants.ChannelReadinessPeriod,
										TimeoutSeconds:      constants.ChannelReadinessTimeout,
										SuccessThreshold:    constants.ChannelReadinessSuccessThreshold,
										FailureThreshold:    constants.ChannelReadinessFailureThreshold,
									},
									Image: "gcr.io",
									Ports: []corev1.ContainerPort{
										{
											Name:          "server",
											ContainerPort: int32(constants.HttpContainerPortNumber),
										},
									},
									Env:             []corev1.EnvVar{},
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.Quantity{
												Format: "300m",
											},
											corev1.ResourceMemory: resource.Quantity{
												Format: "256Mi",
											},
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.Quantity{
												Format: "1",
											},
											corev1.ResourceMemory: resource.Quantity{
												Format: "3Gi",
											},
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      commonconstants.SettingsConfigMapName,
											MountPath: commonconstants.SettingsConfigMapMountPath,
										},
									},
								},
							},
							NodeSelector: map[string]string{},
							Affinity:     &corev1.Affinity{},
							Volumes: []corev1.Volume{
								{
									Name: commonconstants.SettingsConfigMapName,
									VolumeSource: corev1.VolumeSource{
										ConfigMap: &corev1.ConfigMapVolumeSource{
											DefaultMode: &defaultMode,
											LocalObjectReference: corev1.LocalObjectReference{
												Name: commonconstants.SettingsConfigMapName + "-default",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectUpdated: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newDeployment := tt.newDeployment
			if newDeployment == nil {
				newDeployment = getBasicDeployment()
			}
			updatedDeployment, isUpdated := CheckDeploymentChanged(logger, tt.existingDeployment, newDeployment)
			assert.NotNil(t, updatedDeployment)
			assert.Equal(t, tt.expectUpdated, isUpdated)
		})
	}
}

// Tests the CheckServiceChanged functionality.  Note that this is also tested fairly extensively
// as part of the various reconciler tests, and as such the service structs used here are somewhat trivial.
func TestCheckServiceChanged(t *testing.T) {
	internalTrafficPolicy := corev1.ServiceInternalTrafficPolicyCluster
	ipFamilyPolicy := corev1.IPFamilyPolicySingleStack
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name            string
		existingService *corev1.Service
		newService      *corev1.Service
		expectPatch     bool
		expectUpdated   bool
	}{
		{
			name:            "Different Cluster IP",
			existingService: getBasicService(),
			newService:      getBasicService(withDifferentClusterIP),
		},
		{
			name:            "Missing Required Label",
			existingService: getBasicService(),
			newService:      getBasicService(withServiceLabel),
			expectUpdated:   true,
			expectPatch:     true,
		},
		{
			name:            "Missing Required Annotation",
			existingService: getBasicService(),
			newService:      getBasicService(withServiceAnnotation),
			expectUpdated:   true,
			expectPatch:     true,
		},
		{
			name:            "Missing Ports",
			existingService: getBasicService(withoutPorts),
			newService:      getBasicService(),
			expectUpdated:   true,
			expectPatch:     true,
		},
		{
			name:            "Extra Existing Label",
			existingService: getBasicService(withServiceLabel),
			newService:      getBasicService(),
		},
		{
			name:            "Extra Existing Annotation",
			existingService: getBasicService(withServiceAnnotation),
			newService:      getBasicService(),
		},
		{
			name:            "Empty Services",
			existingService: &corev1.Service{},
			newService:      &corev1.Service{},
		},
		{
			name:            "Different Type",
			existingService: getBasicService(withDefaultType),
			newService:      getBasicService(withDifferentType),
		},
		{
			name: "Test with more fields",
			existingService: &corev1.Service{
				TypeMeta: metav1.TypeMeta{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       constants.ServiceKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-cluster-ttt-32603413-receiver",
					Namespace: "knative-eventing",
					Labels: map[string]string{
						constants.KafkaChannelReceiverLabel:  "true",
						constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue,
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP:             "172.20.148.5",
					ClusterIPs:            []string{"172.20.148.5"},
					InternalTrafficPolicy: &internalTrafficPolicy,
					IPFamilies:            []corev1.IPFamily{corev1.IPv4Protocol},
					IPFamilyPolicy:        &ipFamilyPolicy,
					Ports: []corev1.ServicePort{
						{
							Name:       constants.HttpPortName,
							Port:       constants.HttpServicePortNumber,
							TargetPort: intstr.FromInt(constants.HttpContainerPortNumber),
						},
						{
							Name:       constants.MetricsPortName,
							Port:       8081,
							TargetPort: intstr.FromInt(8081),
						},
					},
					Selector: map[string]string{
						constants.AppLabel: "kafka-cluster-ttt-32603413-receiver",
					},
					SessionAffinity: corev1.ServiceAffinityNone,
					Type:            corev1.ServiceTypeClusterIP,
				},
			},
			newService: &corev1.Service{
				TypeMeta: metav1.TypeMeta{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       constants.ServiceKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kafka-cluster-ttt-32603413-receiver",
					Namespace: "knative-eventing",
					Labels: map[string]string{
						constants.KafkaChannelReceiverLabel:  "true",
						constants.K8sAppChannelSelectorLabel: constants.K8sAppChannelSelectorValue,
					},
				},
				Spec: corev1.ServiceSpec{
					IPFamilies:            []corev1.IPFamily{corev1.IPv4Protocol},
					InternalTrafficPolicy: &internalTrafficPolicy,
					IPFamilyPolicy:        &ipFamilyPolicy,
					Ports: []corev1.ServicePort{
						{
							Name:       constants.HttpPortName,
							Port:       constants.HttpServicePortNumber,
							TargetPort: intstr.FromInt(constants.HttpContainerPortNumber),
						},
						{
							Name:       constants.MetricsPortName,
							Port:       8081,
							TargetPort: intstr.FromInt(8081),
						},
					},
					Selector: map[string]string{
						constants.AppLabel: "kafka-cluster-ttt-32603413-receiver",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newService := tt.newService
			if newService == nil {
				newService = getBasicService()
			}
			patch, isUpdated := CheckServiceChanged(logger, tt.existingService, newService)
			assert.Equal(t, tt.expectPatch, patch != nil)
			assert.Equal(t, tt.expectUpdated, isUpdated)
		})
	}
}

func TestCreateJsonPatch(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name        string
		before      interface{}
		after       interface{}
		expectPatch bool
		expectOk    bool
	}{
		{
			name:   "Invalid Content",
			before: math.Inf(1),
			after:  math.Inf(1),
		},
		{
			name:   "No Difference",
			before: getBasicService(),
			after:  getBasicService(),
		},
		{
			name:        "Missing Ports",
			before:      getBasicService(withoutPorts),
			after:       getBasicService(),
			expectPatch: true,
			expectOk:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch, ok := createJsonPatch(logger, tt.before, tt.after)
			assert.Equal(t, tt.expectPatch, patch != nil)
			assert.Equal(t, tt.expectOk, ok)
		})
	}
}

func getBasicDeployment(options ...deploymentOption) *appsv1.Deployment {
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "TestDeployment",
			Namespace:   "TestNamespace",
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      make(map[string]string),
					Annotations: make(map[string]string),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "TestContainerName"},
					},
				},
			},
		},
	}

	// Apply any desired customizations
	for _, option := range options {
		option(deployment)
	}

	return deployment
}

func withDeploymentLabel(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Labels["TestDeploymentLabelName"] = "TestDeploymentLabelValue"
}

func withTemplateLabel(deployment *appsv1.Deployment) {
	deployment.Spec.Template.ObjectMeta.Labels["TestTemplateLabelName"] = "TestTemplateLabelValue"
}

func withDeploymentAnnotation(deployment *appsv1.Deployment) {
	deployment.ObjectMeta.Annotations["TestDeploymentAnnotationName"] = "TestDeploymentAnnotationValue"
}

func withTemplateAnnotation(deployment *appsv1.Deployment) {
	deployment.Spec.Template.ObjectMeta.Annotations["TestTemplateAnnotationName"] = "TestTemplateAnnotationValue"
}

func withoutContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = []corev1.Container{}
}

func withExtraContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = append(
		deployment.Spec.Template.Spec.Containers, corev1.Container{
			Name: "TestExtraContainerName",
		})
}

func withExtraContainerFirst(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = append([]corev1.Container{{
		Name: "TestExtraContainerName",
	}},
		deployment.Spec.Template.Spec.Containers...)
}

func withDifferentContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Name = "TestDifferentContainerName"
}

func withDifferentImage(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Image = "TestNewImage"
}

func getBasicService(options ...serviceOption) *corev1.Service {
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       constants.ServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "TestService",
			Namespace:   "TestNamespace",
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name: "TestServicePort",
			}},
		},
	}

	// Apply any desired customizations
	for _, option := range options {
		option(service)
	}

	return service
}

func withServiceLabel(service *corev1.Service) {
	service.ObjectMeta.Labels["TestServiceLabelName"] = "TestServiceLabelValue"
}

func withServiceAnnotation(service *corev1.Service) {
	service.ObjectMeta.Annotations["TestServiceAnnotationName"] = "TestServiceAnnotationValue"
}

func withDifferentClusterIP(service *corev1.Service) {
	service.Spec.ClusterIP = "DifferentClusterIP"
}

func withoutPorts(service *corev1.Service) {
	service.Spec.Ports = nil
}

func withDefaultType(service *corev1.Service) {
	service.Spec.Type = corev1.ServiceTypeClusterIP
}

func withDifferentType(service *corev1.Service) {
	service.Spec.Type = corev1.ServiceTypeNodePort
}
