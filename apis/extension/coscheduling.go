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

package extension

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationGangPrefix = "gang.scheduling.koordinator.sh"
	// AnnotationGangName specifies the name of the gang
	AnnotationGangName = AnnotationGangPrefix + "/name"

	// AnnotationGangMinNum specifies the minimum number of the gang that can be executed
	AnnotationGangMinNum = AnnotationGangPrefix + "/min-available"

	// AnnotationGangWaitTime specifies gang's max wait time in Permit Stage
	AnnotationGangWaitTime = AnnotationGangPrefix + "/waiting-time"

	// AnnotationGangTotalNum specifies the total children number of the gang
	// If not specified,it will be set with the AnnotationGangMinNum
	AnnotationGangTotalNum = AnnotationGangPrefix + "/total-number"

	// AnnotationGangMode defines the Gang Scheduling operation when failed scheduling
	// Support GangModeStrict and GangModeNonStrict, default is GangModeStrict
	AnnotationGangMode = AnnotationGangPrefix + "/mode"

	// AnnotationGangGroups defines which gangs are bundled as a group
	// The gang will go to bind only all gangs in one group meet the conditions
	AnnotationGangGroups = AnnotationGangPrefix + "/groups"

	// AnnotationGangTimeout means that the entire gang cannot be scheduled due to timeout
	// The annotation is added by the scheduler when the gang times out
	AnnotationGangTimeout = AnnotationGangPrefix + "/timeout"

	// Strict 模式，如果其中一个 pod 调度失败，当前调度周期内，其他已经调度成功的 pod 将会被取消调度，同时正在调度中的 pod 将会在 PreFilter 阶段被拒绝调度。
	// NonStrict 模式，如果其中一个 pod 调度失败，并不会影响其他 pod 参与调度，会继续累计已经被调度的 pod 直到符合 Gang 调度条件。此模式对于 pod 比较多的情况比较友好，但是会增加不同 Gang 调度之间资源死锁的风险。

	GangModeStrict    = "Strict"
	GangModeNonStrict = "NonStrict"

	// WaitingPod 指的是在 permit 阶段等待 Bind 的 pod，
	// 如果一个 permit 扩展返回了 wait，则 Pod 将保持在 permit 阶段，直到被其他 Permit 插件 approve，
	// 如果超时事件发生，wait 状态变成 deny，Pod 将被放回到待调度队列，此时将触发 Unreserve 扩展

	// AnnotationGangMatchPolicy defines the Gang Scheduling operation of taking which status pod into account
	// Support GangMatchPolicyOnlyWaiting, GangMatchPolicyWaitingAndRunning, GangMatchPolicyOnceSatisfied, default is GangMatchPolicyOnceSatisfied
	AnnotationGangMatchPolicy        = AnnotationGangPrefix + "/match-policy"
	GangMatchPolicyOnlyWaiting       = "only-waiting"
	GangMatchPolicyWaitingAndRunning = "waiting-and-running"
	GangMatchPolicyOnceSatisfied     = "once-satisfied"

	// AnnotationAliasGangMatchPolicy defines same match policy but different prefix.
	// Duplicate definitions here are only for compatibility considerations
	AnnotationAliasGangMatchPolicy = "pod-group.scheduling.sigs.k8s.io/match-policy"
)

const (
	// Deprecated: kubernetes-sigs/scheduler-plugins/lightweight-coscheduling
	LabelLightweightCoschedulingPodGroupName = "pod-group.scheduling.sigs.k8s.io/name"
	// Deprecated: kubernetes-sigs/scheduler-plugins/lightweight-coscheduling
	LabelLightweightCoschedulingPodGroupMinAvailable = "pod-group.scheduling.sigs.k8s.io/min-available"
)

func GetMinNum(pod *corev1.Pod) (int, error) {
	minRequiredNum, err := strconv.ParseInt(pod.Annotations[AnnotationGangMinNum], 10, 32)
	if err != nil {
		return 0, err
	}
	return int(minRequiredNum), nil
}

func GetGangName(pod *corev1.Pod) string {
	return pod.Annotations[AnnotationGangName]
}

func GetGangMatchPolicy(pod *corev1.Pod) string {
	policy := pod.Annotations[AnnotationGangMatchPolicy]
	if policy != "" {
		return policy
	}
	return pod.Annotations[AnnotationAliasGangMatchPolicy]
}
