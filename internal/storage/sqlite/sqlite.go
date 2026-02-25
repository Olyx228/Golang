package sqlite

import (
	"UrlScrather/internal/storage"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite" // Используем только modernc (pure Go)
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.sqlite.New"

	// Создаем директорию для БД если она не существует
	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		return nil, fmt.Errorf("%s: failed to create storage directory: %w", op, err)
	}

	// Открываем соединение с SQLite
	db, err := sql.Open("sqlite", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	// Проверяем соединение
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	// Создаем таблицы если их нет
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("%s: failed to create tables: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func createTables(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS url_mappings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		alias TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_alias ON url_mappings(alias);
	`

	_, err := db.Exec(query)
	return err
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) SaveURL(urlToSave string, alias string) (int64, error) {
	const op = "storage.sqlite.SaveURL"

	// Исправляем название таблицы на url_mappings
	stmt, err := s.db.Prepare("INSERT INTO url_mappings(url, alias) VALUES(?, ?)")
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	defer stmt.Close() // Важно закрывать statement

	res, err := stmt.Exec(urlToSave, alias)
	if err != nil {
		// Проверяем ошибку уникальности для modernc.org/sqlite
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return 0, fmt.Errorf("%s: %w", op, storage.ErrURLExists)
		}
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: failed to get last insert id: %w", op, err)
	}

	return id, nil
}

// GetURL возвращает URL по alias
func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.sqlite.GetURL"

	var url string
	err := s.db.QueryRow("SELECT url FROM url_mappings WHERE alias = ?", alias).Scan(&url)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", storage.ErrURLNotFound
		}
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return url, nil
}

// DeleteURL удаляет URL по alias
func (s *Storage) DeleteURL(alias string) (int64, error) {
	const op = "storage.sqlite.DeleteURL"

	result, err := s.db.Exec("DELETE FROM url_mappings WHERE alias = ?", alias)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	if rowsAffected == 0 {
		return 0, storage.ErrURLNotFound
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return id, nil
}
