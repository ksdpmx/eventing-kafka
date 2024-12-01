/*
Copyright 2020 The Knative Authors

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

package kafkachannel

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"reflect"
	"strings"
	"sync"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/event"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
)

// Reconciler Implements controller.Reconciler for KafkaChannel Resources
type Reconciler struct {
	kubeClientset        kubernetes.Interface
	kafkaClientSet       kafkaclientset.Interface
	adminClientType      types.AdminClientType
	adminClient          types.AdminClientInterface
	adminClients         map[string]types.AdminClientInterface
	environment          *env.Environment
	config               *commonconfig.EventingKafkaConfig
	configs              map[string]*commonconfig.EventingKafkaConfig
	kafkachannelLister   kafkalisters.KafkaChannelLister
	kafkachannelInformer cache.SharedIndexInformer
	deploymentLister     appsv1listers.DeploymentLister
	serviceLister        corev1listers.ServiceLister
	adminMutex           *sync.Mutex
	kafkaConfigMapHash   string
}

var (
	_ kafkachannel.Interface = (*Reconciler)(nil) // Verify Reconciler Implements Interface
	_ kafkachannel.Finalizer = (*Reconciler)(nil) // Verify Reconciler Implements Finalizer
)

// SetKafkaAdminClient Clears / Re-Sets The Kafka AdminClient On The Reconciler
//
// Ideally we would re-use the Kafka AdminClient but due to Issues with the Sarama ClusterAdmin we're
// forced to recreate a new connection every time.  We were seeing "broken-pipe" failures (non-recoverable)
// with the ClusterAdmin after periods of inactivity.
//
//	https://github.com/Shopify/sarama/issues/1162
//	https://github.com/Shopify/sarama/issues/866
//
// EventHub AdminClients could be reused, and this is somewhat inefficient for them, but they are very simple
// lightweight REST clients so recreating them isn't a big deal and it simplifies the code significantly to
// not have to support both use cases.
func (r *Reconciler) SetKafkaAdminClient(ctx context.Context) error {
	_ = r.ClearKafkaAdminClient(ctx) // Attempt to close any lingering connections, ignore errors and continue
	var err error
	brokers := strings.Split(r.config.Kafka.Brokers, ",")
	r.adminClient, err = admin.CreateAdminClient(ctx, brokers, r.config.Sarama.Config, r.adminClientType)
	if err != nil {
		logger := logging.FromContext(ctx)
		logger.Error("Failed To Create Kafka AdminClient", zap.Error(err))
	}
	return err
}

func (r *Reconciler) SetKafkaAdminClients(ctx context.Context) error {
	_ = r.ClearKafkaAdminClients(ctx)
	if r.adminClients == nil {
		r.adminClients = make(map[string]types.AdminClientInterface)
	}

	var err error
	for tenant, config := range r.configs {
		brokers := strings.Split(config.Kafka.Brokers, ",")
		r.adminClients[tenant], err = admin.CreateAdminClient(ctx, brokers, config.Sarama.Config, r.adminClientType)
		if err != nil {
			logger := logging.FromContext(ctx)
			logger.Error("Failed To Create Kafka AdminClient", zap.Error(err))
		}
	}
	return err
}

// ClearKafkaAdminClient Clears (Closes) The Reconciler's Kafka AdminClient
func (r *Reconciler) ClearKafkaAdminClient(ctx context.Context) error {
	var err error
	if r.adminClient != nil {
		err = r.adminClient.Close()
		if err != nil {
			logger := logging.FromContext(ctx)
			logger.Error("Failed To Close Kafka AdminClient", zap.Error(err))
		}
		r.adminClient = nil
	}
	return err
}

// ClearKafkaAdminClients Clears (Closes) The Reconciler's Kafka AdminClients
func (r *Reconciler) ClearKafkaAdminClients(ctx context.Context) error {
	var err error
	if r.adminClients != nil && len(r.adminClients) != 0 {
		for _, client := range r.adminClients {
			err = client.Close()
			if err != nil {
				logger := logging.FromContext(ctx)
				logger.Error("Failed To Close Kafka AdminClient", zap.Error(err))
			}
		}
		r.adminClients = nil
	}
	return err
}

// ReconcileKind Implements The Reconciler Interface & Is Responsible For Performing The Reconciliation (Creation)
func (r *Reconciler) ReconcileKind(ctx context.Context, channel *kafkav1beta1.KafkaChannel) reconciler.Event {

	// Get The Logger Via The Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START KAFKA-CHANNEL RECONCILIATION  ==========>")

	// Verify channel is valid.
	channel.SetDefaults(ctx)
	if err := channel.Validate(ctx); err != nil {
		logger.Error("Invalid kafka channel", zap.String("channel", channel.Name), zap.Error(err))
		return err
	}

	// Add The K8S ClientSet To The Reconcile Context
	ctx = context.WithValue(ctx, kubeclient.Key{}, r.kubeClientset)
	// Add A Channel-Specific Logger To The Context
	ctx = logging.WithLogger(ctx, util.ChannelLogger(logger, channel).Sugar())

	// Don't let another goroutine clear out the admin client while we're using it in this one
	r.adminMutex.Lock()
	defer r.adminMutex.Unlock()

	// Create New Kafka AdminClients For Each Reconciliation Attempt
	err := r.SetKafkaAdminClients(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = r.ClearKafkaAdminClients(ctx) }() // Ignore errors as nothing else can be done

	// Reset The Channel's Status Conditions To Unknown (Addressable, Topic, Service, Deployment, etc...)
	channel.Status.InitializeConditions()

	// Perform The KafkaChannel Reconciliation & Handle Error Response
	logger.Info("Channel Owned By Controller - Reconciling", zap.Any("Channel.Spec", channel.Spec))
	err = r.reconcile(ctx, channel)
	if err != nil {
		logger.Error("Failed To Reconcile KafkaChannel", zap.Any("Channel", channel), zap.Error(err))
		return err
	}

	// Return Success
	logger.Info("Successfully Reconciled KafkaChannel", zap.Any("Channel", channel))
	channel.Status.ObservedGeneration = channel.Generation
	return reconciler.NewEvent(
		corev1.EventTypeNormal, event.KafkaChannelReconciled.String(),
		"KafkaChannel Reconciled Successfully: \"%s/%s\"", channel.Namespace, channel.Name,
	)
}

// FinalizeKind Implements The Finalizer Interface & Is Responsible For Performing The Finalization (Topic Deletion)
func (r *Reconciler) FinalizeKind(ctx context.Context, channel *kafkav1beta1.KafkaChannel) reconciler.Event {

	// Get The Logger Via The Context
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("<==========  START KAFKA-CHANNEL FINALIZATION  ==========>")

	// Add The K8S ClientSet To The Reconcile Context
	ctx = context.WithValue(ctx, kubeclient.Key{}, r.kubeClientset)
	// Add A Channel-Specific Logger To The Context
	ctx = logging.WithLogger(ctx, util.ChannelLogger(logger, channel).Sugar())

	// Don't let another goroutine clear out the admin client while we're using it in this one
	r.adminMutex.Lock()
	defer r.adminMutex.Unlock()

	// Create New Kafka AdminClients For Each Reconciliation Attempt
	err := r.SetKafkaAdminClients(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = r.ClearKafkaAdminClients(ctx) }() // Ignore errors as nothing else can be done

	// Finalize The Dispatcher (Manual Finalization Due To Cross-Namespace Ownership)
	err = r.finalizeDispatcher(ctx, channel)
	if err != nil {
		logger.Info("Failed To Finalize KafkaChannel", zap.Error(err))
		return fmt.Errorf(constants.FinalizationFailedError)
	}

	// Finalize The Kafka Topic
	err = r.finalizeKafkaTopic(ctx, channel)
	if err != nil {
		logger.Error("Failed To Finalize KafkaChannel", zap.Error(err))
		return fmt.Errorf(constants.FinalizationFailedError)
	}

	// Return Success
	logger.Info("Successfully Finalized KafkaChannel")
	return reconciler.NewEvent(
		corev1.EventTypeNormal, event.KafkaChannelFinalized.String(), "KafkaChannel Finalized Successfully: \"%s/%s\"",
		channel.Namespace, channel.Name,
	)
}

// Perform The Actual Channel Reconciliation
func (r *Reconciler) reconcile(ctx context.Context, channel *kafkav1beta1.KafkaChannel) error {
	tenant := channel.Spec.Tenant

	// NOTE - The sequential order of reconciliation must be "Topic" then "Channel / Dispatcher" in order for the
	//        EventHub Cache to know the dynamically determined EventHub Namespace / Kafka Secret selected for the topic.

	// Reconcile The KafkaChannel's Kafka Topic
	err := r.reconcileKafkaTopic(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	//
	// This implementation is based on the "consolidated" KafkaChannel, and thus we're using
	// their Status tracking even though it does not align with the distributed channel's
	// architecture.  We get our Kafka configuration from the "Kafka Secrets" and not a
	// ConfigMap.  Therefore, we will instead check the Kafka Secret associated with the
	// KafkaChannel here.
	//
	if len(r.configs[tenant].Kafka.AuthSecretName) > 0 {
		channel.Status.MarkConfigTrue()
	} else {
		channel.Status.MarkConfigFailed(event.KafkaSecretReconciled.String(), "No Kafka Secret For KafkaChannel")
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Reconcile the Receiver Deployment/Service
	receiverError := r.reconcileReceiver(ctx, channel)

	// Reconcile The KafkaChannel's Channel & Dispatcher Deployment/Service
	channelError := r.reconcileChannel(ctx, channel)

	dispatcherError := r.reconcileDispatcher(ctx, channel)
	if channelError != nil || dispatcherError != nil || receiverError != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Reconcile The KafkaChannel Itself (MetaData, etc...)
	err = r.reconcileKafkaChannel(ctx, channel)
	if err != nil {
		return fmt.Errorf(constants.ReconciliationFailedError)
	}

	// Return Success
	return nil
}

// updateKafkaConfig is the callback function that handles changes to our ConfigMap
func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) error {
	logger := logging.FromContext(ctx)

	if r == nil {
		return fmt.Errorf("reconciler is nil (possible startup race condition)")
	}

	if configMap == nil {
		return fmt.Errorf("nil configMap passed to updateKafkaConfig")
	}

	// Validate & Reload ConfigMap
	if configMap.Data == nil {
		return fmt.Errorf("configMap.Data is mandatory")
	}
	for tenant, tenantConfig := range configMap.Data {
		if len(tenantConfig) == 0 {
			return fmt.Errorf("configMap.Data[%s] is mandatory", tenant)
		}
		data := make(map[string]string)
		if err := yaml.Unmarshal([]byte(tenantConfig), &data); err != nil {
			return err
		}
		if _, ok := data["eventing-kafka"]; !ok {
			return fmt.Errorf("eventing-kafka config is mandatory")
		}
		if _, ok := data["sarama"]; !ok {
			return fmt.Errorf("sarama config is mandatory")
		}

		// reload config from configmap
		configuration, err := sarama.LoadSettings(ctx, constants.Component, data, sarama.LoadAuthConfig)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed To Reload Eventing-Kafka Config for tenant %s", tenant), zap.Error(err))
		}
		r.configs[tenant] = configuration
	}

	logger.Warn("Updating managed configmaps for different tenants")

	// checking wiri
	oldMCMs, err := r.kubeClientset.CoreV1().ConfigMaps(r.environment.SystemNamespace).List(ctx, metav1.ListOptions{LabelSelector: constants.IsManagedLabel + "=true"})
	if err != nil {
		return err
	}
	for _, oldMCM := range oldMCMs.Items {
		if !strings.HasPrefix(oldMCM.Name, commonconstants.SettingsConfigMapName+"-") {
			logger.Warn("Non-managed configmap is found in wiri, ignoring", zap.String("configmap", oldMCM.Name))
			continue
		}
		if _, ok := configMap.Data[oldMCM.Name[len(commonconstants.SettingsConfigMapName+"-"):]]; !ok {
			logger.Warn("Managed configmap is not found in wisb, deleting", zap.String("configmap", oldMCM.Name))
			if err := r.kubeClientset.CoreV1().ConfigMaps(r.environment.SystemNamespace).Delete(ctx, oldMCM.Name, metav1.DeleteOptions{}); err != nil {
				return err
			}
			logger.Warn("Dangling managed configmap has been deleted", zap.String("configmap", oldMCM.Name))
		}
	}

	// checking wisb
	for tenant, tenantConfig := range configMap.Data {
		data := make(map[string]string)
		// err should have been caught by the validation above
		if err := yaml.Unmarshal([]byte(tenantConfig), &data); err != nil {
			return err
		}

		newMCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      commonconstants.SettingsConfigMapName + "-" + tenant,
				Namespace: r.environment.SystemNamespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         corev1.SchemeGroupVersion.String(),
						Kind:               "ConfigMap",
						Name:               commonconstants.SettingsConfigMapName,
						UID:                configMap.UID,
						Controller:         pointer.BoolPtr(true),
						BlockOwnerDeletion: pointer.BoolPtr(true),
					},
				},
				Labels: map[string]string{
					constants.IsManagedLabel:     "true",
					constants.KafkaChannelTenant: tenant,
				},
			},
			Data: data,
		}

		oldMCM, err := r.kubeClientset.CoreV1().ConfigMaps(r.environment.SystemNamespace).Get(ctx, commonconstants.SettingsConfigMapName+"-"+tenant, metav1.GetOptions{})
		isMCMUpdated := false
		if err != nil {
			if errors.IsNotFound(err) {
				logger.Warn("Managed configmap is not found", zap.String("configmap", commonconstants.SettingsConfigMapName+"-"+tenant))
				if _, err := r.kubeClientset.CoreV1().ConfigMaps(r.environment.SystemNamespace).Create(ctx, newMCM, metav1.CreateOptions{}); err != nil {
					return err
				}
				isMCMUpdated = true
				logger.Warn("Managed configmap has been created", zap.String("configmap", newMCM.Name))
			} else {
				return err
			}
		} else {
			// TODO check metadata as well
			if !reflect.DeepEqual(oldMCM.Data, newMCM.Data) {
				logger.Warn("Managed configmap data has changed", zap.String("configmap", newMCM.Name))
				if _, err := r.kubeClientset.CoreV1().ConfigMaps(r.environment.SystemNamespace).Update(ctx, newMCM, metav1.UpdateOptions{}); err != nil {
					return err
				}
				isMCMUpdated = true
				logger.Warn("Managed configmap has been updated", zap.String("configmap", newMCM.Name))
			}
		}

		// reconcile receiver when needed
		if isMCMUpdated && r.configs[tenant] != nil {
			dummyChannel := &kafkav1beta1.KafkaChannel{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: r.environment.SystemNamespace,
				},
				Spec: kafkav1beta1.KafkaChannelSpec{
					Tenant: tenant,
				},
			}
			logger.Warn("Reconciling receiver for tenant", zap.String("tenant", tenant))
			if err := r.reconcileReceiver(ctx, dummyChannel); err != nil {
				return err
			}
		}
	}

	logger.Warn("Reloading Kafka configuration")

	return nil
}
