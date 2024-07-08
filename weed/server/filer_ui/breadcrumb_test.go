package filer_ui

import (
	"reflect"
	"testing"
)

func TestToBreadcrumb(t *testing.T) {
	type args struct {
		fullpath string
	}
	tests := []struct {
		name       string
		args       args
		wantCrumbs []Breadcrumb
	}{
		{
			name: "empty",
			args: args{
				fullpath: "",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
			},
		},
		{
			name: "test1",
			args: args{
				fullpath: "/",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
			},
		},
		{
			name: "test2",
			args: args{
				fullpath: "/abc",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
				{
					Name: "abc",
					Link: "/abc/",
				},
			},
		},
		{
			name: "test3",
			args: args{
				fullpath: "/abc/def",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
				{
					Name: "abc",
					Link: "/abc/",
				},
				{
					Name: "def",
					Link: "/abc/def/",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotCrumbs := ToBreadcrumb(tt.args.fullpath); !reflect.DeepEqual(gotCrumbs, tt.wantCrumbs) {
				t.Errorf("ToBreadcrumb() = %v, want %v", gotCrumbs, tt.wantCrumbs)
			}
		})
	}
}
