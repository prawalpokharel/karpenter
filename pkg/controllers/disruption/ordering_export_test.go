/*
Copyright The Kubernetes Authors.

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

package disruption

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// Test-only accessors for the candidate ordering study in ordering_study_test.go. They expose the per-candidate
// gate that SingleNodeConsolidation applies inside its walk, so the study can walk candidates in other orders
// without copying that gate.

// ReschedulablePodsForStudy returns the pods a disruption of this candidate would evict.
func (c *Candidate) ReschedulablePodsForStudy() []*corev1.Pod {
	return c.reschedulablePods
}

// ConsolidationGateForStudy runs the same per-candidate checks as SingleNodeConsolidation.ComputeCommands:
// the best-case threshold, the scheduling simulation with price filtering, and the policy evaluator.
// It returns a NoOp command when the candidate does not pass.
func (s *SingleNodeConsolidation) ConsolidationGateForStudy(ctx context.Context, candidate *Candidate) (Command, error) {
	if !s.evaluator.CanPassThreshold(candidate) {
		return Command{}, nil
	}
	cmd, err := s.computeConsolidation(ctx, candidate)
	if err != nil || cmd.Decision() == NoOpDecision {
		return Command{}, err
	}
	if approved, _ := s.evaluator.ApproveCommand(ctx, cmd); !approved {
		return Command{}, nil
	}
	return cmd, nil
}
