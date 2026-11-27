package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSHA256PasswordValidatorHashesAndValidatesSHA256Password(t *testing.T) {
	validator := NewSHA256PasswordValidator()
	hash, err := validator.HashPassword("secret")
	require.NoError(t, err)
	require.Len(t, hash, 64)
	digest := sha256.Sum256([]byte("secret"))
	require.Equal(t, hex.EncodeToString(digest[:]), hash)
	require.True(t, validator.ValidatePassword("secret", hash, nil))
	require.False(t, validator.ValidatePassword("wrong", hash, nil))
	require.False(t, validator.ValidatePassword("secret", "not-a-sha256-hash", nil))
}

func TestPasswordValidatorFactoryCreatesSHA256PasswordValidator(t *testing.T) {
	validator := (&PasswordValidatorFactory{}).CreateValidator("sha256_password")
	require.IsType(t, &SHA256PasswordValidator{}, validator)
}
