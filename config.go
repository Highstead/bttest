package bttest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

var ErrEmptyEnvironmentVariable = fmt.Errorf("empty environment variable")

type Config struct {
	IsDebug     bool
	AppProfiles []string `json:"appProfiles"`
	Project     string
	Instance    string
}

func GetConfig(path string) (*Config, error) {
	c := &Config{
		IsDebug: true,
	}
	err := envParseBool("debug", &c.IsDebug)
	if err != nil && err != ErrEmptyEnvironmentVariable {
		return nil, err
	}

	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(file, &c)

	return c, nil
}

func envParseBool(str string, input *bool) error {
	b := os.Getenv(str)
	if b != "" {
		ret, err := strconv.ParseBool(b)
		if err != nil {
			*input = ret
			return nil
		}
		return err
	}
	return ErrEmptyEnvironmentVariable
}

func envParseStr(str string, input *string) error {
	s := os.Getenv(str)
	if s != "" {
		*input = s
		return nil
	}
	return ErrEmptyEnvironmentVariable
}
