package libmigrate

import "testing"

func TestFullTableName(t *testing.T) {
	cases := []struct {
		schema, table, expected string
	}{
		{
			schema:   "",
			table:    "t",
			expected: `"t"`,
		},
		{
			schema:   "public",
			table:    "tname",
			expected: `public."tname"`,
		},
	}

	for _, c := range cases {
		t.Run(c.expected, func(t *testing.T) {
			db := &dbWrapperImpl{
				tableSchema: c.schema,
				tableName:   c.table,
			}
			actual := db.fullTableName()
			if actual != c.expected {
				t.Errorf("Expected `%s`, got `%s`", c.expected, actual)
			}
		})
	}
}
