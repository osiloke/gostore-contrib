package firestoredb

import (
	"reflect"
	"testing"
)

func Test_formatted(t *testing.T) {
	type args struct {
		prefix  string
		field   string
		valRune []rune
	}
	tests := []struct {
		name string
		args args
		want query
	}{
		{
			"name < 200",
			args{"<", "name", []rune("n300")},
			query{"name", "<=", 300},
		}, {
			"name < 200",
			args{"<", "name", []rune("d200")},
			query{"name", "<", "200"},
		},
		{
			"name < 200",
			args{"<", "name", []rune("n200")},
			query{"name", "<=", 200},
		},
		{
			"name !! 200",
			args{"!!", "name", []rune("w200")},
			query{"name", "!!", "200"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatted(tt.args.prefix, tt.args.field, tt.args.valRune); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("formatted() = %v, want %v", got, tt.want)
			}
		})
	}
}
