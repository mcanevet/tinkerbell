/*
Copyright 2022 Tinkerbell.
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

// Package netboot resolves the BIOS attribute PATCH body needed to enable or disable UEFI HTTP
// Boot and/or legacy PXE boot capability, based on a fingerprint match against a machine's
// current BIOS configuration.
package netboot

import "fmt"

// fingerprint keys are attribute names that, if present in a GetBiosConfiguration() result,
// identify this table as applicable. Extend with one entry per additional vendor/board as
// fleets need them — do not attempt to support unknown BIOS/vendor combinations silently.
//
// httpEnabled/httpDisabled and pxeEnabled/pxeDisabled are independent attribute sets: HTTP Boot
// and legacy PXE are separate capabilities that can be on or off in any combination. NetworkStack
// and BootModeSelect are prerequisites shared by both protocols, so they're only asserted on the
// enabled side of each set — disabling one protocol must not turn off the network stack the
// other protocol may still depend on.
var tables = []struct {
	fingerprint  string // attribute name unique enough to identify this BIOS/vendor
	httpEnabled  map[string]string
	httpDisabled map[string]string
	pxeEnabled   map[string]string
	pxeDisabled  map[string]string
}{
	{
		fingerprint: "IPv4HTTPSupport", // Supermicro H12SSW-NTR / AMI Aptio, confirmed io14-72
		httpEnabled: map[string]string{
			"NetworkStack":    "Enabled",
			"BootModeSelect":  "UEFI",
			"IPv4HTTPSupport": "Enabled",
			"IPv6HTTPSupport": "Enabled",
		},
		httpDisabled: map[string]string{
			"IPv4HTTPSupport": "Disabled",
			"IPv6HTTPSupport": "Disabled",
		},
		pxeEnabled: map[string]string{
			"NetworkStack":   "Enabled",
			"BootModeSelect": "UEFI",
			"IPv4PXESupport": "Enabled",
		},
		pxeDisabled: map[string]string{
			"IPv4PXESupport": "Disabled",
		},
	},
}

// Attributes returns the BIOS attribute PATCH body for the requested HTTP Boot / PXE Boot
// enabled states, selecting the table whose fingerprint attribute is present in current (the
// machine's live GetBiosConfiguration result). httpEnabled and pxeEnabled are independent: a nil
// pointer leaves that protocol's attributes untouched, so either, both, or neither may be set.
// Returns an error if neither is set, or if no known table matches — silently no-op'ing on an
// unrecognized BIOS is worse than a clear, actionable failure.
func Attributes(httpEnabled, pxeEnabled *bool, current map[string]string) (map[string]string, error) {
	if httpEnabled == nil && pxeEnabled == nil {
		return nil, fmt.Errorf("at least one of httpEnabled or pxeEnabled must be set")
	}

	for _, t := range tables {
		if _, ok := current[t.fingerprint]; !ok {
			continue
		}

		attrs := map[string]string{}
		if httpEnabled != nil {
			mergeInto(attrs, pick(*httpEnabled, t.httpEnabled, t.httpDisabled))
		}
		if pxeEnabled != nil {
			mergeInto(attrs, pick(*pxeEnabled, t.pxeEnabled, t.pxeDisabled))
		}
		return attrs, nil
	}
	return nil, fmt.Errorf("no known BIOS attribute mapping for this machine (fingerprint attributes not found in GetBiosConfiguration result)")
}

func pick(enabled bool, onTrue, onFalse map[string]string) map[string]string {
	if enabled {
		return onTrue
	}
	return onFalse
}

func mergeInto(dst, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}
