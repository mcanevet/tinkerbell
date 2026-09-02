package netboot

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func boolPtr(b bool) *bool { return &b }

func TestAttributes(t *testing.T) {
	tests := map[string]struct {
		httpEnabled *bool
		pxeEnabled  *bool
		current     map[string]string
		want        map[string]string
		wantError   bool
	}{
		"enable http only, pxe untouched": {
			httpEnabled: boolPtr(true),
			current:     map[string]string{"IPv4HTTPSupport": "Disabled", "IPv4PXESupport": "Enabled"},
			want: map[string]string{
				"NetworkStack":    "Enabled",
				"BootModeSelect":  "UEFI",
				"IPv4HTTPSupport": "Enabled",
				"IPv6HTTPSupport": "Enabled",
			},
		},
		"disable http only, pxe untouched": {
			httpEnabled: boolPtr(false),
			current:     map[string]string{"IPv4HTTPSupport": "Enabled"},
			want: map[string]string{
				"IPv4HTTPSupport": "Disabled",
				"IPv6HTTPSupport": "Disabled",
			},
		},
		"enable pxe only, http untouched": {
			pxeEnabled: boolPtr(true),
			current:    map[string]string{"IPv4HTTPSupport": "Enabled", "IPv4PXESupport": "Disabled"},
			want: map[string]string{
				"NetworkStack":   "Enabled",
				"BootModeSelect": "UEFI",
				"IPv4PXESupport": "Enabled",
			},
		},
		"disable pxe only, http untouched": {
			pxeEnabled: boolPtr(false),
			current:    map[string]string{"IPv4HTTPSupport": "Enabled"},
			want: map[string]string{
				"IPv4PXESupport": "Disabled",
			},
		},
		"enable both http and pxe": {
			httpEnabled: boolPtr(true),
			pxeEnabled:  boolPtr(true),
			current:     map[string]string{"IPv4HTTPSupport": "Disabled"},
			want: map[string]string{
				"NetworkStack":    "Enabled",
				"BootModeSelect":  "UEFI",
				"IPv4HTTPSupport": "Enabled",
				"IPv6HTTPSupport": "Enabled",
				"IPv4PXESupport":  "Enabled",
			},
		},
		"disable http while enabling pxe": {
			httpEnabled: boolPtr(false),
			pxeEnabled:  boolPtr(true),
			current:     map[string]string{"IPv4HTTPSupport": "Enabled"},
			want: map[string]string{
				"NetworkStack":    "Enabled",
				"BootModeSelect":  "UEFI",
				"IPv4HTTPSupport": "Disabled",
				"IPv6HTTPSupport": "Disabled",
				"IPv4PXESupport":  "Enabled",
			},
		},
		"neither set returns error": {
			current:   map[string]string{"IPv4HTTPSupport": "Enabled"},
			wantError: true,
		},
		"unknown fingerprint returns error": {
			httpEnabled: boolPtr(true),
			current:     map[string]string{"SomeUnrelatedAttribute": "value"},
			wantError:   true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Attributes(tt.httpEnabled, tt.pxeEnabled, tt.current)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected nil err, got: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("unexpected attributes (-want +got):\n%s", diff)
			}
		})
	}
}
