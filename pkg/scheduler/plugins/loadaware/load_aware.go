/*
Copyright 2022 The Koordinator Authors.

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

package loadaware

import (
	"context"
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/koordinator-sh/koordinator/apis/extension"
	slov1alpha1 "github.com/koordinator-sh/koordinator/apis/slo/v1alpha1"
	slolisters "github.com/koordinator-sh/koordinator/pkg/client/listers/slo/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config/validation"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext"
	frameworkexthelper "github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext/helper"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/loadaware/estimator"
)

const (
	Name                                    = "LoadAwareScheduling"
	ErrReasonUsageExceedThreshold           = "node(s) %s usage exceed threshold"
	ErrReasonAggregatedUsageExceedThreshold = "node(s) %s aggregated usage exceed threshold"
	ErrReasonFailedEstimatePod
)

const (
	// DefaultMilliCPURequest defines default milli cpu request number.
	DefaultMilliCPURequest int64 = 250 // 0.25 core
	// DefaultMemoryRequest defines default memory request size.
	DefaultMemoryRequest int64 = 200 * 1024 * 1024 // 200 MB
	// DefaultNodeMetricReportInterval defines the default koodlet report NodeMetric interval.
	DefaultNodeMetricReportInterval = 60 * time.Second
)

var (
	_ framework.EnqueueExtensions = &Plugin{}

	_ framework.FilterPlugin  = &Plugin{}
	_ framework.ScorePlugin   = &Plugin{}
	_ framework.ReservePlugin = &Plugin{}
)

type Plugin struct {
	handle           framework.Handle
	args             *config.LoadAwareSchedulingArgs
	podLister        corev1listers.PodLister
	nodeMetricLister slolisters.NodeMetricLister
	estimator        estimator.Estimator
	podAssignCache   *podAssignCache
}

// 负载感知调度只是打分，实际调度还是需要满足 pod 的 request 小于 nodeInfo.Allocatable - nodeInfo.Requested，由社区 noderesources 插件实现，
// 并不是真的 “pod 的 request 小于节点运行时剩余资源就可以调度上去”，只是参考运行时负载打分看节点优先级

//

func New(args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pluginArgs, ok := args.(*config.LoadAwareSchedulingArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type LoadAwareSchedulingArgs, got %T", args)
	}

	if err := validation.ValidateLoadAwareSchedulingArgs(pluginArgs); err != nil {
		return nil, err
	}

	frameworkExtender, ok := handle.(frameworkext.ExtendedHandle)
	if !ok {
		return nil, fmt.Errorf("want handle to be of type frameworkext.ExtendedHandle, got %T", handle)
	}

	// podAssignCache 记录了 pod 和节点的绑定情况，通过 informer 已经被调度成功的 pod 也会被更新到 podAssignCache
	assignCache := newPodAssignCache()
	podInformer := frameworkExtender.SharedInformerFactory().Core().V1().Pods()
	frameworkexthelper.ForceSyncFromInformer(context.TODO().Done(), frameworkExtender.SharedInformerFactory(), podInformer.Informer(), assignCache)
	podLister := podInformer.Lister()
	nodeMetricLister := frameworkExtender.KoordinatorSharedInformerFactory().Slo().V1alpha1().NodeMetrics().Lister()

	estimator, err := estimator.NewEstimator(pluginArgs, handle)
	if err != nil {
		return nil, err
	}

	return &Plugin{
		handle:           handle,
		args:             pluginArgs,
		podLister:        podLister,
		nodeMetricLister: nodeMetricLister,
		estimator:        estimator,
		podAssignCache:   assignCache,
	}, nil
}

func (p *Plugin) Name() string { return Name }

func (p *Plugin) EventsToRegister() []framework.ClusterEvent {
	// To register a custom event, follow the naming convention at:
	// https://github.com/kubernetes/kubernetes/blob/e1ad9bee5bba8fbe85a6bf6201379ce8b1a611b1/pkg/scheduler/eventhandlers.go#L415-L422
	// 调度器通过 nodemetrics CR 来感知节点的负载情况
	// koordlet 需要将节点负载情况更新到 nodemetrics CR 对象的 status 里
	gvk := fmt.Sprintf("nodemetrics.%v.%v", slov1alpha1.GroupVersion.Version, slov1alpha1.GroupVersion.Group)
	return []framework.ClusterEvent{
		{Resource: framework.GVK(gvk), ActionType: framework.Add | framework.Update | framework.Delete},
	}
}

func (p *Plugin) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	// 直接忽略 daemonset pod
	if isDaemonSetPod(pod.OwnerReferences) {
		return nil
	}

	// 每个 node 的 nodemetric 和对应 node 对象的名字是相同的
	nodeMetric, err := p.nodeMetricLister.Get(node.Name)
	if err != nil {
		// For nodes that lack load information, fall back to the situation where there is no load-aware scheduling.
		// Some nodes in the cluster do not install the koordlet, but users newly created Pod use koord-scheduler to schedule,
		// and the load-aware scheduling itself is an optimization, so we should skip these nodes.
		if errors.IsNotFound(err) {
			return nil
		}
		return framework.NewStatus(framework.Error, err.Error())
	}

	if p.args.FilterExpiredNodeMetrics != nil && *p.args.FilterExpiredNodeMetrics &&
		p.args.NodeMetricExpirationSeconds != nil && isNodeMetricExpired(nodeMetric, *p.args.NodeMetricExpirationSeconds) {
		return nil
	}

	filterProfile := generateUsageThresholdsFilterProfile(node, p.args)
	if len(filterProfile.ProdUsageThresholds) > 0 && extension.GetPodPriorityClassWithDefault(pod) == extension.PriorityProd {
		// 如果 pod 是 prod 的，并且节点的 prod 工作负载使用资源超过了阈值，则 pod 不能调度到该节点上
		status := p.filterProdUsage(node, nodeMetric, filterProfile.ProdUsageThresholds)
		if !status.IsSuccess() {
			return status
		}
	} else {
		// 检查节点是否达到了总的使用上限，filterProfile.AggregatedUsage 是更高级的上限计量方式，采用一段时间内的 P50、P95 的指标作为实际资源使用情况
		// 这些个指标在 nodeMetric CR 对象里已经算好了，直接取值就行
		var usageThresholds map[corev1.ResourceName]int64
		if filterProfile.AggregatedUsage != nil {
			usageThresholds = filterProfile.AggregatedUsage.UsageThresholds
		} else {
			usageThresholds = filterProfile.UsageThresholds
		}
		if len(usageThresholds) > 0 {
			status := p.filterNodeUsage(node, nodeMetric, filterProfile)
			if !status.IsSuccess() {
				return status
			}
		}
	}

	return nil
}

func (p *Plugin) filterNodeUsage(node *corev1.Node, nodeMetric *slov1alpha1.NodeMetric, filterProfile *usageThresholdsFilterProfile) *framework.Status {
	if nodeMetric.Status.NodeMetric == nil {
		return nil
	}

	var usageThresholds map[corev1.ResourceName]int64
	if filterProfile.AggregatedUsage != nil {
		usageThresholds = filterProfile.AggregatedUsage.UsageThresholds
	} else {
		usageThresholds = filterProfile.UsageThresholds
	}

	for resourceName, threshold := range usageThresholds {
		if threshold == 0 {
			continue
		}
		allocatable, err := p.estimator.EstimateNode(node)
		if err != nil {
			klog.ErrorS(err, "Failed to EstimateNode", "node", node.Name)
			return nil
		}
		total := allocatable[resourceName]
		if total.IsZero() {
			continue
		}
		// TODO(joseph): maybe we should estimate the Pod that just be scheduled that have not reported
		var nodeUsage *slov1alpha1.ResourceMap
		if filterProfile.AggregatedUsage != nil {
			nodeUsage = getTargetAggregatedUsage(
				nodeMetric,
				filterProfile.AggregatedUsage.UsageAggregatedDuration,
				filterProfile.AggregatedUsage.UsageAggregationType,
			)
		} else {
			nodeUsage = &nodeMetric.Status.NodeMetric.NodeUsage
		}
		if nodeUsage == nil {
			continue
		}

		used := nodeUsage.ResourceList[resourceName]
		usage := int64(math.Round(float64(used.MilliValue()) / float64(total.MilliValue()) * 100))
		if usage >= threshold {
			reason := ErrReasonUsageExceedThreshold
			if filterProfile.AggregatedUsage != nil {
				reason = ErrReasonAggregatedUsageExceedThreshold
			}
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf(reason, resourceName))
		}
	}
	return nil
}

func (p *Plugin) filterProdUsage(node *corev1.Node, nodeMetric *slov1alpha1.NodeMetric, prodUsageThresholds map[corev1.ResourceName]int64) *framework.Status {
	if len(nodeMetric.Status.PodsMetric) == 0 {
		return nil
	}

	// TODO(joseph): maybe we should estimate the Pod that just be scheduled that have not reported
	podMetrics := buildPodMetricMap(p.podLister, nodeMetric, true)
	prodPodUsages, _ := sumPodUsages(podMetrics, nil)
	for resourceName, threshold := range prodUsageThresholds {
		if threshold == 0 {
			continue
		}
		allocatable, err := p.estimator.EstimateNode(node)
		if err != nil {
			klog.ErrorS(err, "Failed to EstimateNode", "node", node.Name)
			return nil
		}
		total := allocatable[resourceName]
		if total.IsZero() {
			continue
		}
		used := prodPodUsages[resourceName]
		usage := int64(math.Round(float64(used.MilliValue()) / float64(total.MilliValue()) * 100))
		if usage >= threshold {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf(ErrReasonUsageExceedThreshold, resourceName))
		}
	}
	return nil
}

func (p *Plugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (p *Plugin) Reserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	// 绑定中 pod 也需要被标记成 assigned，后续 pod 才会将绑定中的 pod 考虑在 node 使用情况内
	p.podAssignCache.assign(nodeName, pod)
	return nil
}

func (p *Plugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	p.podAssignCache.unAssign(nodeName, pod)
}

func (p *Plugin) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := p.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}
	node := nodeInfo.Node()
	if node == nil {
		return 0, framework.NewStatus(framework.Error, "node not found")
	}
	nodeMetric, err := p.nodeMetricLister.Get(nodeName)
	if err != nil {
		// caused by load-aware scheduling itself is an optimization,
		// so we should skip the node and score the node 0
		if errors.IsNotFound(err) {
			return 0, nil
		}
		return 0, framework.NewStatus(framework.Error, err.Error())
	}
	if p.args.NodeMetricExpirationSeconds != nil && isNodeMetricExpired(nodeMetric, *p.args.NodeMetricExpirationSeconds) {
		return 0, nil
	}

	prodPod := extension.GetPodPriorityClassWithDefault(pod) == extension.PriorityProd && p.args.ScoreAccordingProdUsage
	podMetrics := buildPodMetricMap(p.podLister, nodeMetric, prodPod)

	// 估计要调度的目标 pod 的用量
	estimatedUsed, err := p.estimator.EstimatePod(pod)
	if err != nil {
		return 0, nil
	}

	// assignedPodEstimatedUsed 应该是该节点上所有已经绑定了的 pod 的估计使用量之和（看起来如果有问题会直接使用这个）
	// 如果 pod 是 prod 类型，assignedPodEstimatedUsed 不会包含非 prod pod 的结果
	assignedPodEstimatedUsed, estimatedPods := p.estimatedAssignedPodUsed(nodeName, nodeMetric, podMetrics, prodPod)
	for resourceName, value := range assignedPodEstimatedUsed {
		estimatedUsed[resourceName] += value
	}

	// estimatedPodActualUsages 应该是 assignedPodEstimatedUsed 里每个 pod 对应的实际使用量之和
	// podActualUsages 应该是该节点上所有没在 estimatedPodActualUsages 里的 pod 的实际使用量之和
	podActualUsages, estimatedPodActualUsages := sumPodUsages(podMetrics, estimatedPods)
	if prodPod {
		// 如果是 prod 的 pod，用该节点上所有已经绑定了的 prod 类型 pod 的估计使用量之和加上其他 pod 的实际使用量之和
		for resourceName, quantity := range podActualUsages {
			estimatedUsed[resourceName] += getResourceValue(resourceName, quantity)
		}
	} else {
		if nodeMetric.Status.NodeMetric != nil {
			var nodeUsage *slov1alpha1.ResourceMap
			if scoreWithAggregation(p.args.Aggregated) {
				nodeUsage = getTargetAggregatedUsage(nodeMetric, &p.args.Aggregated.ScoreAggregatedDuration, p.args.Aggregated.ScoreAggregationType)
			} else {
				nodeUsage = &nodeMetric.Status.NodeMetric.NodeUsage
			}
			if nodeUsage != nil {
				// 如果不是 prod 的 pod，estimatedUsed 结果为，当前 pod 和该节点上所有已经绑定了的 pod 的估计使用量之和
				// 加上 ‘该节点实际统计使用量减去 “该节点上所有已经绑定了的 pod 的实际使用量之和” 之差’
				for resourceName, quantity := range nodeUsage.ResourceList {
					if q := estimatedPodActualUsages[resourceName]; !q.IsZero() {
						quantity = quantity.DeepCopy()
						if quantity.Cmp(q) >= 0 {
							quantity.Sub(q)
						}
					}
					estimatedUsed[resourceName] += getResourceValue(resourceName, quantity)
				}
			}
		}
	}

	allocatable, err := p.estimator.EstimateNode(node)
	if err != nil {
		return 0, nil
	}
	score := loadAwareSchedulingScorer(p.args.ResourceWeights, estimatedUsed, allocatable)
	return score, nil
}

func (p *Plugin) estimatedAssignedPodUsed(nodeName string, nodeMetric *slov1alpha1.NodeMetric, podMetrics map[string]corev1.ResourceList, filterProdPod bool) (map[corev1.ResourceName]int64, sets.String) {
	estimatedUsed := make(map[corev1.ResourceName]int64)
	estimatedPods := sets.NewString()
	var nodeMetricUpdateTime time.Time
	if nodeMetric.Status.UpdateTime != nil {
		nodeMetricUpdateTime = nodeMetric.Status.UpdateTime.Time
	}
	nodeMetricReportInterval := getNodeMetricReportInterval(nodeMetric)

	p.podAssignCache.lock.RLock()
	defer p.podAssignCache.lock.RUnlock()
	for _, assignInfo := range p.podAssignCache.podInfoItems[nodeName] {
		if filterProdPod && extension.GetPodPriorityClassWithDefault(assignInfo.pod) != extension.PriorityProd {
			continue
		}
		podName := getPodNamespacedName(assignInfo.pod.Namespace, assignInfo.pod.Name)
		podUsage := podMetrics[podName]
		if len(podUsage) == 0 ||
			missedLatestUpdateTime(assignInfo.timestamp, nodeMetricUpdateTime) ||
			stillInTheReportInterval(assignInfo.timestamp, nodeMetricUpdateTime, nodeMetricReportInterval) ||
			(scoreWithAggregation(p.args.Aggregated) &&
				getTargetAggregatedUsage(nodeMetric, &p.args.Aggregated.ScoreAggregatedDuration, p.args.Aggregated.ScoreAggregationType) == nil) {
			estimated, err := p.estimator.EstimatePod(assignInfo.pod)
			if err != nil {
				continue
			}
			for resourceName, value := range estimated {
				if quantity, ok := podUsage[resourceName]; ok {
					usage := getResourceValue(resourceName, quantity)
					if usage > value {
						value = usage
					}
				}
				estimatedUsed[resourceName] += value
			}
			estimatedPods.Insert(podName)
		}
	}
	return estimatedUsed, estimatedPods
}

func loadAwareSchedulingScorer(resToWeightMap, used map[corev1.ResourceName]int64, allocatable corev1.ResourceList) int64 {
	var nodeScore, weightSum int64
	for resourceName, weight := range resToWeightMap {
		resourceScore := leastRequestedScore(used[resourceName], getResourceValue(resourceName, allocatable[resourceName]))
		nodeScore += resourceScore * weight
		weightSum += weight
	}
	return nodeScore / weightSum
}

func leastRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		return 0
	}

	// framework.MaxNodeScore 是打分的最大值
	return ((capacity - requested) * framework.MaxNodeScore) / capacity
}
