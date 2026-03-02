package save_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"UrlScrather/internal/http-server/handlers/url/save"
	"UrlScrather/internal/http-server/handlers/url/save/mocks"
)

func TestSaveHandler(t *testing.T) {
	cases := []struct {
		name      string
		alias     string
		url       string
		respError string
		mockError error
	}{
		{
			name:  "Success",
			alias: "test_alias",
			url:   "https://google.com",
		},
		{
			name:  "Empty alias",
			alias: "",
			url:   "https://google.com",
		},
		{
			name:      "Empty URL",
			url:       "",
			alias:     "some_alias",
			respError: "field URL is a required field",
		},
		{
			name:      "Invalid URL",
			url:       "some invalid URL",
			alias:     "some_alias",
			respError: "field URL is not a valid URL",
		},
		{
			name:  "SaveURL Error",
			alias: "test_alias",
			url:   "https://google.com",
			// Исправлено: "failed to add url" -> "failed to save url"
			respError: "failed to save url",
			mockError: errors.New("unexpected error"),
		},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			urlSaverMock := mocks.NewURLSaver(t)

			// Настраиваем мок для всех случаев, кроме тех, где ожидается ошибка валидации
			// (в этих случаях SaveURL не должен вызываться)
			if tc.respError == "" || tc.mockError != nil {
				// Для случая с ошибкой SaveURL или успешного сохранения
				if tc.name != "Empty URL" && tc.name != "Invalid URL" {
					urlSaverMock.On("SaveURL", tc.url, mock.AnythingOfType("string")).
						Return(int64(1), tc.mockError).
						Once()
				}
			}

			logger := slog.New(slog.DiscardHandler)
			handler := save.New(logger, urlSaverMock)

			input := fmt.Sprintf(`{"url": "%s", "alias": "%s"}`, tc.url, tc.alias)

			req, err := http.NewRequest(http.MethodPost, "/save", bytes.NewReader([]byte(input)))
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)

			var resp save.Response
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			require.Equal(t, tc.respError, resp.Error)
		})
	}
}
