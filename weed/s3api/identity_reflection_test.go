package s3api

import (
	"reflect"
	"testing"
)

// TestIdentityFieldsForS3TablesReflection ensures the identity struct keeps the
// fields relied on by s3tables.getIdentityPrincipalArn, getIdentityPolicyNames,
// and getIdentityClaims via reflection.
func TestIdentityFieldsForS3TablesReflection(t *testing.T) {
	typ := reflect.TypeOf(Identity{})
	checkField(t, typ, "PrincipalArn", reflect.String)
	field, ok := typ.FieldByName("PolicyNames")
	if !ok {
		t.Fatalf("Identity.PolicyNames missing")
	}
	if field.Type.Kind() != reflect.Slice {
		t.Fatalf("Identity.PolicyNames must be a slice, got %s", field.Type.Kind())
	}
	field, ok = typ.FieldByName("Claims")
	if !ok {
		t.Fatalf("Identity.Claims missing")
	}
	if field.Type.Kind() != reflect.Map || field.Type.Key().Kind() != reflect.String {
		t.Fatalf("Identity.Claims must be map[string]..., got %s/%s", field.Type.Kind(), field.Type.Key().Kind())
	}
}

func checkField(t *testing.T, typ reflect.Type, name string, kind reflect.Kind) {
	t.Helper()
	field, ok := typ.FieldByName(name)
	if !ok {
		t.Fatalf("Identity.%s missing", name)
	}
	if field.Type.Kind() != kind {
		t.Fatalf("Identity.%s must be %s, got %s", name, kind, field.Type.Kind())
	}
}
