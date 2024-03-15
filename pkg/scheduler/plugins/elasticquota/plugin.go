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

package elasticquota

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	v1 "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling"
	apiv1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config/validation"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext"
	frameworkexthelper "github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext/helper"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/elasticquota/core"
	reservationutil "github.com/koordinator-sh/koordinator/pkg/util/reservation"
	"github.com/koordinator-sh/koordinator/pkg/util/transformer"
)

const (
	Name                              = "ElasticQuota"
	MigrateDefaultQuotaGroupsPodCycle = 1 * time.Second
	postFilterKey                     = "PostFilter" + Name
)

type PostFilterState struct {
	skip               bool
	quotaInfo          *core.QuotaInfo
	used               corev1.ResourceList
	nonPreemptibleUsed corev1.ResourceList
	usedLimit          corev1.ResourceList
}

func (p *PostFilterState) Clone() framework.StateData {
	return &PostFilterState{
		quotaInfo:          p.quotaInfo,
		used:               p.used.DeepCopy(),
		nonPreemptibleUsed: p.nonPreemptibleUsed.DeepCopy(),
		usedLimit:          p.usedLimit.DeepCopy(),
	}
}

type Plugin struct {
	handle            framework.Handle
	client            versioned.Interface
	pluginArgs        *config.ElasticQuotaArgs
	quotaLister       v1alpha1.ElasticQuotaLister
	quotaInformer     cache.SharedIndexInformer
	podLister         v1.PodLister
	pdbLister         policylisters.PodDisruptionBudgetLister
	nodeLister        v1.NodeLister
	groupQuotaManager *core.GroupQuotaManager

	quotaManagerLock sync.RWMutex
	// groupQuotaManagersForQuotaTree store the GroupQuotaManager of all quota trees. The key is the quota tree id
	groupQuotaManagersForQuotaTree map[string]*core.GroupQuotaManager

	quotaToTreeMapLock sync.RWMutex
	// quotaToTreeMap store the relationship of quota and quota tree
	// the key is the quota name, the value is the tree id
	quotaToTreeMap map[string]string
}

var (
	_ framework.EnqueueExtensions = &Plugin{}
	_ framework.PreFilterPlugin   = &Plugin{}
	_ framework.PostFilterPlugin  = &Plugin{}
	_ framework.ReservePlugin     = &Plugin{}
)

func New(args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pluginArgs, ok := args.(*config.ElasticQuotaArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type GangSchedulingArgs, got %T", args)
	}
	if err := validation.ValidateElasticQuotaArgs(pluginArgs); err != nil {
		return nil, err
	}

	client, ok := handle.(versioned.Interface)
	if !ok {
		kubeConfig := *handle.KubeConfig()
		kubeConfig.ContentType = runtime.ContentTypeJSON
		kubeConfig.AcceptContentTypes = runtime.ContentTypeJSON
		client = versioned.NewForConfigOrDie(&kubeConfig)
	}

	scheSharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	transformer.SetupElasticQuotaTransformers(scheSharedInformerFactory)
	elasticQuotaInformer := scheSharedInformerFactory.Scheduling().V1alpha1().ElasticQuotas()
	informer := elasticQuotaInformer.Informer()
	if err := informer.AddIndexers(map[string]cache.IndexFunc{
		"annotation.namespaces": func(obj interface{}) ([]string, error) {
			eq, ok := obj.(*apiv1alpha1.ElasticQuota)
			if !ok {
				return []string{}, nil
			}
			if len(eq.Annotations) == 0 || eq.Annotations[extension.AnnotationQuotaNamespaces] == "" {
				return []string{}, nil
			}
			return extension.GetAnnotationQuotaNamespaces(eq), nil
		},
	}); err != nil {
		return nil, err
	}

	elasticQuota := &Plugin{
		handle:                         handle,
		client:                         client,
		pluginArgs:                     pluginArgs,
		podLister:                      handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		quotaInformer:                  informer,
		quotaLister:                    elasticQuotaInformer.Lister(),
		pdbLister:                      getPDBLister(handle),
		nodeLister:                     handle.SharedInformerFactory().Core().V1().Nodes().Lister(),
		groupQuotaManagersForQuotaTree: make(map[string]*core.GroupQuotaManager),
		quotaToTreeMap:                 make(map[string]string),
	}
	elasticQuota.groupQuotaManager = core.NewGroupQuotaManager("", pluginArgs.SystemQuotaGroupMax, pluginArgs.DefaultQuotaGroupMax)

	elasticQuota.quotaToTreeMap[extension.DefaultQuotaName] = ""
	elasticQuota.quotaToTreeMap[extension.SystemQuotaName] = ""

	ctx := context.TODO()

	// 很多事情都是在 informer 里通过对象事件触发的，调度时使用的是 informer 更新后的结果

	elasticQuota.createRootQuotaIfNotPresent()
	elasticQuota.createSystemQuotaIfNotPresent()
	elasticQuota.createDefaultQuotaIfNotPresent()
	frameworkexthelper.ForceSyncFromInformer(ctx.Done(), scheSharedInformerFactory, informer, cache.ResourceEventHandlerFuncs{
		AddFunc:    elasticQuota.OnQuotaAdd,
		UpdateFunc: elasticQuota.OnQuotaUpdate,
		DeleteFunc: elasticQuota.OnQuotaDelete,
	})

	nodeInformer := handle.SharedInformerFactory().Core().V1().Nodes().Informer()
	frameworkexthelper.ForceSyncFromInformer(ctx.Done(), handle.SharedInformerFactory(), nodeInformer, cache.ResourceEventHandlerFuncs{
		AddFunc:    elasticQuota.OnNodeAdd,
		UpdateFunc: elasticQuota.OnNodeUpdate,
		DeleteFunc: elasticQuota.OnNodeDelete,
	})

	podInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	frameworkexthelper.ForceSyncFromInformer(ctx.Done(), handle.SharedInformerFactory(), podInformer, cache.ResourceEventHandlerFuncs{
		AddFunc:    elasticQuota.OnPodAdd,
		UpdateFunc: elasticQuota.OnPodUpdate,
		DeleteFunc: elasticQuota.OnPodDelete,
	})

	elasticQuota.migrateDefaultQuotaGroupsPod()

	return elasticQuota, nil
}

func (g *Plugin) Start() {
	go wait.Until(g.migrateDefaultQuotaGroupsPod, MigrateDefaultQuotaGroupsPodCycle, nil)
	klog.Infof("start migrate pod from defaultQuotaGroup")
}

func (g *Plugin) NewControllers() ([]frameworkext.Controller, error) {
	quotaOverUsedRevokeController := NewQuotaOverUsedRevokeController(g)
	elasticQuotaController := NewElasticQuotaController(g)
	return []frameworkext.Controller{g, quotaOverUsedRevokeController, elasticQuotaController}, nil
}

func (g *Plugin) Name() string {
	return Name
}

func (g *Plugin) EventsToRegister() []framework.ClusterEvent {
	// To register a custom event, follow the naming convention at:
	// https://git.k8s.io/kubernetes/pkg/scheduler/eventhandlers.go#L403-L410
	eqGVK := fmt.Sprintf("elasticquotas.v1alpha1.%v", scheduling.GroupName)
	return []framework.ClusterEvent{
		// 只有 pod 删除或者 elastic quota 更新了之后，才触发 pod 的重调度
		{Resource: framework.Pod, ActionType: framework.Delete},
		{Resource: framework.GVK(eqGVK), ActionType: framework.All},
	}
}

// 要使用 elastic quota 全部功能只需要部署 koord-scheduler、koord-manager 就可以

func (g *Plugin) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	quotaName, treeID := g.getPodAssociateQuotaNameAndTreeID(pod)
	if quotaName == "" {
		// 如果 pod 没有对应的 elastic quota，则跳过 post filter 阶段
		g.skipPostFilterState(cycleState)
		return nil, framework.NewStatus(framework.Success, "")
	}

	mgr := g.GetGroupQuotaManagerForTree(treeID)
	if mgr == nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Could not find the specified ElasticQuotaManager for quota: %v, tree: %v", quotaName, treeID))
	}
	if g.pluginArgs.EnableRuntimeQuota {
		// 重新计算 runtime 值，算法可以参考 https://koordinator.sh/zh-Hans/docs/designs/multi-hierarchy-elastic-quota-management/#calculate-runtimequota
		mgr.RefreshRuntime(quotaName)
	}

	// 算完之后获取最新的对应 elastic quota 信息
	quotaInfo := mgr.GetQuotaInfoByName(quotaName)
	if quotaInfo == nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Could not find the specified ElasticQuota"))
	}

	// 把 elastic quota 状态快照保存在 cycleState 里的 postFilterKey 部分
	// 每个 plugin 的 cycleState 应该是独立的
	state := g.snapshotPostFilterState(quotaInfo, cycleState)

	podRequest, _ := core.PodRequestsAndLimits(pod)

	// 这里的 state.used 应该指的是已经 assigned 的 pod request 之和
	used := quotav1.Mask(quotav1.Add(podRequest, state.used), quotav1.ResourceNames(podRequest))
	// state.usedLimit 为 runtime 或者 max
	if isLessEqual, exceedDimensions := quotav1.LessThanOrEqual(used, state.usedLimit); !isLessEqual {
		return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient quotas, "+
			"quotaName: %v, runtime: %v, used: %v, pod's request: %v, exceedDimensions: %v",
			quotaName, printResourceList(state.usedLimit), printResourceList(state.used), printResourceList(podRequest), exceedDimensions))
	}

	if extension.IsPodNonPreemptible(pod) {
		quotaMin := state.quotaInfo.CalculateInfo.Min
		nonPreemptibleUsed := state.nonPreemptibleUsed
		addNonPreemptibleUsed := quotav1.Mask(quotav1.Add(podRequest, nonPreemptibleUsed), quotav1.ResourceNames(podRequest))
		if isLessEqual, exceedDimensions := quotav1.LessThanOrEqual(addNonPreemptibleUsed, quotaMin); !isLessEqual {
			return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient non-preemptible quotas, "+
				"quotaName: %v, min: %v, nonPreemptibleUsed: %v, pod's request: %v, exceedDimensions: %v",
				quotaName, printResourceList(quotaMin), printResourceList(nonPreemptibleUsed), printResourceList(podRequest), exceedDimensions))
		}
	}

	if g.pluginArgs.EnableCheckParentQuota {
		return nil, g.checkQuotaRecursive(quotaName, []string{quotaName}, podRequest)
	}

	return nil, framework.NewStatus(framework.Success, "")
}

func (g *Plugin) PreFilterExtensions() framework.PreFilterExtensions {
	return g
}

// AddPod is called by the framework while trying to evaluate the impact
// of adding podToAdd to the node while scheduling podToSchedule.
func (g *Plugin) AddPod(ctx context.Context, state *framework.CycleState, podToSchedule *corev1.Pod, podInfoToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	if reservationutil.IsReservePod(podInfoToAdd.Pod) {
		return nil
	}

	postFilterState, err := getPostFilterState(state)
	if err != nil {
		klog.ErrorS(err, "Failed to read postFilterState from cycleState", "elasticQuotaSnapshotKey", postFilterState)
		return framework.NewStatus(framework.Error, err.Error())
	}

	if postFilterState.skip {
		return framework.NewStatus(framework.Success, "")
	}

	// 主要服务于抢占的逻辑，抢占的时候也要结合 elastic quota 的条件考虑
	// quotaInfo 里的 pod cache 保存了所有 pod 的信息
	if postFilterState.quotaInfo.IsPodExist(podInfoToAdd.Pod) {
		podReq, _ := core.PodRequestsAndLimits(podInfoToAdd.Pod)
		postFilterState.used = quotav1.Add(postFilterState.used, podReq)
	}
	return framework.NewStatus(framework.Success, "")
}

// RemovePod is called by the framework while trying to evaluate the impact
// of removing podToRemove from the node while scheduling podToSchedule.
func (g *Plugin) RemovePod(ctx context.Context, state *framework.CycleState, podToSchedule *corev1.Pod, podInfoToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	if reservationutil.IsReservePod(podInfoToRemove.Pod) {
		return nil
	}

	postFilterState, err := getPostFilterState(state)
	if err != nil {
		klog.ErrorS(err, "Failed to read postFilterState from cycleState", "elasticQuotaSnapshotKey", postFilterState)
		return framework.NewStatus(framework.Error, err.Error())
	}

	if postFilterState.skip {
		return framework.NewStatus(framework.Success, "")
	}

	// 主要服务于抢占的逻辑，抢占的时候也要结合 elastic quota 的条件考虑
	if postFilterState.quotaInfo.IsPodExist(podInfoToRemove.Pod) {
		podReq, _ := core.PodRequestsAndLimits(podInfoToRemove.Pod)
		postFilterState.used = quotav1.SubtractWithNonNegativeResult(postFilterState.used, podReq)
	}
	return framework.NewStatus(framework.Success, "")
}

// PostFilter modify the defaultPreemption, only allow pods in the same quota can preempt others.
func (g *Plugin) PostFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	pe := preemption.Evaluator{
		PluginName: Name,
		Handler:    g.handle,
		PodLister:  g.podLister,
		PdbLister:  g.pdbLister,
		State:      state,

		// 这里的接口是自己实现的，跟 default-preemption 不一样
		Interface: g,
	}

	result, status := pe.Preempt(ctx, pod, filteredNodeStatusMap)
	if status.Message() != "" {
		return result, framework.NewStatus(status.Code(), "preemption: "+status.Message())
	}
	return result, status
}

func (g *Plugin) Reserve(ctx context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) *framework.Status {
	quotaName, treeID := g.getPodAssociateQuotaNameAndTreeID(p)
	if quotaName == "" {
		return framework.NewStatus(framework.Success, "")
	}

	mgr := g.GetGroupQuotaManagerForTree(treeID)
	if mgr == nil {
		klog.Errorf("failed reserve pod %v/%v, quota manager not found, quota: %v, tree: %v", p.Namespace, p.Name, quotaName, treeID)
		return framework.NewStatus(framework.Error, fmt.Sprintf("quota manager not found, quota: %v, tree: %v", quotaName, treeID))
	}

	mgr.ReservePod(quotaName, p)
	return framework.NewStatus(framework.Success, "")
}

func (g *Plugin) Unreserve(ctx context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) {
	quotaName, treeID := g.getPodAssociateQuotaNameAndTreeID(p)
	if quotaName == "" {
		return
	}

	mgr := g.GetGroupQuotaManagerForTree(treeID)
	if mgr == nil {
		klog.Errorf("failed unreserve pod %v/%v, quota manager not found, quota: %v, tree: %s", p.Namespace, p.Name, quotaName, treeID)
		return
	}
	mgr.UnreservePod(quotaName, p)
}
