package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env         string `yaml:"env" env:"ENV" env-default:"local"`
	StoragePath string `yaml:"storage_path" env:"STORAGE_PATH" env-required:"true"`
	HTTPServer  `yaml:"http_server"`
}

type HTTPServer struct {
	Address     string        `yaml:"address" env-default:"localhost:8080"`
	Timeout     time.Duration `yaml:"timeout" env-default:"4s"`
	IdleTimeout time.Duration `yaml:"idle_timeout" env-default:"60s"`
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		possiblePaths := []string{
			"./config/local.yaml",
			"./config/config.yaml",
			"./config.yml",
			"./config.yaml",
		}

		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				configPath = path
				log.Printf("Using config file: %s", configPath)
				break
			}
		}

		if configPath == "" {
			log.Fatal("CONFIG_PATH is not set and no config file found in default locations")
		}
	}

	// Проверяем существование файла
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("Config file does not exist: %s", configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("Cannot read config: %s", err)
	}

	return &cfg
}
