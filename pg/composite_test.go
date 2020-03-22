package pg_test

import (
	"testing"

	"github.com/gopub/sqlx/pg"
	"github.com/stretchr/testify/require"
)

func TestParseCompositeFields(t *testing.T) {
	t.Run("Single", func(t *testing.T) {
		res, err := pg.ParseCompositeFields("(abc)")
		require.NoError(t, err)
		require.Equal(t, []string{"abc"}, res)

		res, err = pg.ParseCompositeFields("(abc\")")
		require.Error(t, err)
	})

	t.Run("Multiple", func(t *testing.T) {
		res, err := pg.ParseCompositeFields("(abc,123)")
		require.NoError(t, err)
		require.Equal(t, []string{"abc", "123"}, res)

		res, err = pg.ParseCompositeFields("(abc,)")
		require.NoError(t, err)
		require.Equal(t, []string{"abc", ""}, res)
	})

	t.Run("Embedded", func(t *testing.T) {
		res, err := pg.ParseCompositeFields("(abc,123,\"(19,20)\")")
		require.NoError(t, err)
		require.Equal(t, []string{"abc", "123", "(19,20)"}, res)

		res, err = pg.ParseCompositeFields("(\"(19,20)\",abc,123,)")
		require.NoError(t, err)
		require.Equal(t, []string{"(19,20)", "abc", "123", ""}, res)
	})

	t.Run("Quoted", func(t *testing.T) {
		res, err := pg.ParseCompositeFields("(\"abc\"\", \",123)")
		require.NoError(t, err)
		require.Equal(t, []string{"abc\", ", "123"}, res)
	})
}
