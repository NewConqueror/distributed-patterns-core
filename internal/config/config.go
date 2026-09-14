package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port           string
	DBHost         string
	DBPort         string
	DBUser         string
	DBPassword     string
	DBName         string
	DBSSLMode      string
	DatabaseURL    string
	DBMaxOpenConns int
	DBMaxIdleConns int
}

func Load() *Config {
	return &Config{
		Port:           getEnv("PORT", "8080"),
		DBHost:         os.Getenv("DB_HOST"),
		DBPort:         getEnv("DB_PORT", "5432"),
		DBUser:         getEnv("DB_USER", "postgres"),
		DBPassword:     os.Getenv("DB_PASSWORD"),
		DBName:         getEnv("DB_NAME", "flashsale"),
		DBSSLMode:      getEnv("DB_SSLMODE", "disable"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		DBMaxOpenConns: getEnvAsInt("DB_MAX_OPEN_CONNS", 0), // 0 = unlimited by default (shows connection pool exhaustion)
		DBMaxIdleConns: getEnvAsInt("DB_MAX_IDLE_CONNS", 10),
	}
}

func (c *Config) GetDSN() string {
	if c.DatabaseURL != "" {
		return c.DatabaseURL
	}

	// If no host/password specified, use local Unix socket if available (common for Linux local dev)
	if c.DBHost == "" && c.DBPassword == "" {
		return fmt.Sprintf("host=/var/run/postgresql dbname=%s sslmode=%s", c.DBName, c.DBSSLMode)
	}

	host := c.DBHost
	if host == "" {
		host = "localhost"
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
