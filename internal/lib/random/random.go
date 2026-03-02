package random

import (
	"crypto/rand"
	"math/big"
)

// NewRandomString generates random string with given size using crypto/rand
func NewRandomString(size int) string {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")

	b := make([]rune, size)
	for i := range b {
		// Генерируем криптостойкое случайное число
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			// В случае ошибки используем fallback (но на практике её не будет)
			panic(err)
		}
		b[i] = chars[n.Int64()]
	}

	return string(b)
}
