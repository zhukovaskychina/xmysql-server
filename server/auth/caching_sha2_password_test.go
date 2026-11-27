package auth

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCachingSHA2PasswordValidatorHashesAndValidatesStage2Password(t *testing.T) {
	validator := NewCachingSHA2PasswordValidator()
	hash, err := validator.HashPassword("secret")
	require.NoError(t, err)
	require.Len(t, hash, 64)
	_, err = hex.DecodeString(hash)
	require.NoError(t, err)
	require.True(t, validator.ValidatePassword("secret", hash, nil))
	require.False(t, validator.ValidatePassword("wrong", hash, nil))
}
