package kube

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestMACAddrs(t *testing.T) {
	tests := map[string]struct {
		hw   client.Object
		want []string
	}{
		"not a v1alpha1.Hardware object": {hw: &v1alpha1.Workflow{}, want: nil},
		"2 MACs": {hw: &v1alpha1.Hardware{
			Spec: v1alpha1.HardwareSpec{
				Interfaces: []v1alpha1.Interface{
					{
						DHCP: &v1alpha1.DHCP{
							MAC: "00:00:00:00:00:00",
						},
					},
					{
						DHCP: &v1alpha1.DHCP{
							MAC: "00:00:00:00:00:01",
						},
					},
					{
						DHCP: &v1alpha1.DHCP{},
					},
				},
			},
		}, want: []string{"00:00:00:00:00:00", "00:00:00:00:00:01"}},
		"no interfaces": {hw: &v1alpha1.Hardware{}, want: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			macs := MACAddrs(tc.hw)
			if diff := cmp.Diff(macs, tc.want); diff != "" {
				t.Errorf("unexpected MACs (+want -got):\n%s", diff)
			}
		})
	}
}

func TestIPAddrs(t *testing.T) {
	tests := map[string]struct {
		hw   client.Object
		want []string
	}{
		"not a v1alpha1.Hardware object": {hw: &v1alpha1.Workflow{}, want: nil},
		"2 IPs": {hw: &v1alpha1.Hardware{
			Spec: v1alpha1.HardwareSpec{
				Interfaces: []v1alpha1.Interface{
					{
						DHCP: &v1alpha1.DHCP{
							IP: &v1alpha1.IP{
								Address: "192.168.2.1",
							},
						},
					},
					{
						DHCP: &v1alpha1.DHCP{
							IP: &v1alpha1.IP{
								Address: "192.168.2.2",
							},
						},
					},
					{
						DHCP: &v1alpha1.DHCP{},
					},
					{
						DHCP: &v1alpha1.DHCP{
							IP: &v1alpha1.IP{},
						},
					},
				},
			},
		}, want: []string{"192.168.2.1", "192.168.2.2"}},
		"no interfaces": {hw: &v1alpha1.Hardware{}, want: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := IPAddrs(tc.hw)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected IPs (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkflowByAgentIDFunc(t *testing.T) {
	cases := []struct {
		name           string
		input          client.Object
		wantStateAddrs []string
	}{
		{
			"noworkflow",
			&v1alpha1.Hardware{},
			nil,
		},
		{
			"emptyworkflow",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{},
			},
			[]string{},
		},
		{
			"pendingworkflow",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{
					State:   v1alpha1.WorkflowStatePending,
					AgentID: "agent1",
					Tasks: []v1alpha1.Task{
						{
							AgentID: "agent1",
						},
					},
				},
			},
			[]string{"agent1"},
		},
		{
			"runningworkflow",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{
					State:   v1alpha1.WorkflowStateRunning,
					AgentID: "agent1",
					Tasks: []v1alpha1.Task{
						{
							AgentID: "agent1",
						},
						{
							AgentID: "agent2",
						},
					},
				},
			},
			[]string{"agent1"},
		},
		{
			"completeworkflow",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{
					State: v1alpha1.WorkflowStateSuccess,
					Tasks: []v1alpha1.Task{
						{
							AgentID: "agent1",
						},
					},
				},
			},
			[]string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStateAddrs := WorkflowAgentID(tc.input)
			if !reflect.DeepEqual(tc.wantStateAddrs, gotStateAddrs) {
				t.Errorf("Unexpected WorkflowByAgentIDFunc workflow response: wanted %#v, got %#v", tc.wantStateAddrs, gotStateAddrs)
			}
		})
	}
}

func TestWorkflowHardwareMapAgentIDs(t *testing.T) {
	cases := []struct {
		name    string
		input   client.Object
		wantIDs []string
	}{
		{
			"not a workflow",
			&v1alpha1.Hardware{},
			nil,
		},
		{
			"no hardware map",
			&v1alpha1.Workflow{},
			[]string{},
		},
		{
			"single device, not yet rendered",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{State: v1alpha1.WorkflowStateAwaitingCheckIn},
				Spec: v1alpha1.WorkflowSpec{
					HardwareMap: map[string]string{"device_1": "aa:bb:cc:dd:ee:ff"},
				},
			},
			[]string{"aa:bb:cc:dd:ee:ff"},
		},
		{
			"multiple devices, not yet rendered",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{State: v1alpha1.WorkflowStateAwaitingCheckIn},
				Spec: v1alpha1.WorkflowSpec{
					HardwareMap: map[string]string{
						"device_1": "aa:bb:cc:dd:ee:ff",
						"device_2": "11:22:33:44:55:66",
					},
				},
			},
			[]string{"11:22:33:44:55:66", "aa:bb:cc:dd:ee:ff"},
		},
		{
			"single device, already rendered - not re-discoverable via hardware map",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{State: v1alpha1.WorkflowStatePending},
				Spec: v1alpha1.WorkflowSpec{
					HardwareMap: map[string]string{"device_1": "aa:bb:cc:dd:ee:ff"},
				},
			},
			[]string{},
		},
		{
			"single device, boot preparing - still discoverable before Status.AgentID ever gets set",
			&v1alpha1.Workflow{
				Status: v1alpha1.WorkflowStatus{State: v1alpha1.WorkflowStatePreparing},
				Spec: v1alpha1.WorkflowSpec{
					HardwareMap: map[string]string{"device_1": "aa:bb:cc:dd:ee:ff"},
				},
			},
			[]string{"aa:bb:cc:dd:ee:ff"},
		},
		{
			"single device, zero-value State (e.g. a permanently disabled Workflow) - still discoverable",
			&v1alpha1.Workflow{
				Spec: v1alpha1.WorkflowSpec{
					HardwareMap: map[string]string{"device_1": "aa:bb:cc:dd:ee:ff"},
				},
			},
			[]string{"aa:bb:cc:dd:ee:ff"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := WorkflowHardwareMapAgentIDs(tc.input)
			sort.Strings(got)
			if diff := cmp.Diff(tc.wantIDs, got); diff != "" {
				t.Errorf("WorkflowHardwareMapAgentIDs() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
