package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
)

type config struct {
	Listen ListenConfig

	ClientConnInfo string

	StartupParameters map[string]string
	Databases VirtualDatabaseConfiguration

	Prometheus PrometheusConfig
}

var Config = config{
	// These are the defaults

	Listen:	ListenConfig{6433, "localhost", true},

	ClientConnInfo: "host=localhost port=5432 sslmode=disable",

	StartupParameters: nil,
	Databases: nil,

	Prometheus: PrometheusConfig{
		Enabled: false,
		Listen: ListenConfig{},
	},
}

func readIntValue(dst *int, val interface{}, option string) error {
	var err error

	switch val := val.(type) {
	case float64:
		if math.Trunc(val) == val {
			*dst = int(val)
		} else {
			err = fmt.Errorf("input must be an integer")
		}
	case string:
		*dst, err = strconv.Atoi(val)
	default:
		err = fmt.Errorf("input must be an integer")
	}
	if err != nil {
		return fmt.Errorf("invalid value for option %q: %s", option, err.Error())
	}
	return nil
}

func readTextValue(dst *string, val interface{}, option string) error {
	var err error

	switch val := val.(type) {
	case string:
		*dst = val
		err = nil
	default:
		err = fmt.Errorf("input must be a text string")
	}
	if err != nil {
		return fmt.Errorf("invalid value for option %q: %s", option, err.Error())
	}
	return nil
}

func readBooleanValue(dst *bool, val interface{}, option string) error {
	var err error

	switch val := val.(type) {
	case bool:
		*dst = val
		err = nil
	default:
		err = fmt.Errorf("input must be a boolean")
	}
	if err != nil {
		return fmt.Errorf("invalid value for option %q: %s", option, err.Error())
	}
	return nil
}

func readListenSection(c *ListenConfig, val interface{}, option string) error {
	data, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf(`section %q must be a JSON object`, option)
	}
	for key, value := range data {
		var err error

		switch key {
		case "port":
			err = readIntValue(&c.Port, value, option + ".port")
		case "host":
			err = readTextValue(&c.Host, value, option + ".host")
		case "keepalive":
			err = readBooleanValue(&c.KeepAlive, value, option + ".keepalive")
		default:
			err = fmt.Errorf("unrecognized configuration option %q", option+"."+key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func readConnectSection(c *config, val interface{}) error {
	data, ok := val.(string)
	if !ok {
		return fmt.Errorf(`section "connect" must be a connection string`)
	}
	c.ClientConnInfo = data
	return nil
}

func readStartupParameterSection(c *config, val interface{}) error {
	data, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf(`section "startup_parameters" must be a set of key-value pairs`)
	}
	c.StartupParameters = make(map[string]string)
	for k, v := range data {
		vs, ok := v.(string)
		if !ok {
			return fmt.Errorf(`all startup parameters must be strings`)
		}
		c.StartupParameters[k] = vs
	}
	return nil
}

func readAuthSection(c *AuthConfig, val interface{}, option string) error {
	data, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf(`section %q must be a JSON object`, option)
	}

	for key, value := range data {
		var err error

		switch key {
		case "method":
			err = readTextValue(&c.method, value, option+".method")
		case "user":
			err = readTextValue(&c.user, value, option+".user")
		case "password":
			err = readTextValue(&c.password, value, option+".password")
		default:
			err = fmt.Errorf("unrecognized configuration option %q", option+"."+key)
		}
		if err != nil {
			return err
		}
	}

	switch c.method {
	case "md5":
	case "trust":
	default:
		return fmt.Errorf("unrecognized authentication method %q in %q", c.method, option)
	}

	return nil
}

func readDatabaseSection(c *config, val interface{}) error {
	array, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf(`section "databases" must be a JSON array`)
	}

	for dbindex, el := range array {
		data, ok := el.(map[string]interface{})
		if !ok {
			return fmt.Errorf(`elements within the "databases" array must be JSON objects`)
		}

		option := fmt.Sprintf("databases[%d]", dbindex)
		var db virtualDatabase

		for key, value := range data {
			var err error

			switch key {
			case "name":
				err = readTextValue(&db.name, value, option+".name")
			case "auth":
				err = readAuthSection(&db.auth, value, option+".auth")
			default:
				err = fmt.Errorf("unrecognized configuration option %q", option+"."+key)
			}
			if err != nil {
				return err
			}
		}

		for _, pedb := range c.Databases {
			if pedb.name == db.name {
				return fmt.Errorf("database name %q is not unique", db.name)
			}
		}
		c.Databases = append(c.Databases, db)
	}

	return nil
}

func readPrometheusSection(c *config, val interface{}) error {
	data, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf(`section "prometheus" must be a JSON object`)
	}
	for key, value := range data {
		var err error

		switch key {
		case "listen":
			err = readListenSection(&c.Prometheus.Listen, value, "prometheus.listen")
			c.Prometheus.Enabled = true
		default:
			err = fmt.Errorf("unrecognized configuration option %q", "prometheus." + key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}


func readConfigFile(filename string) error {
	var ci interface{}

	fh, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fh.Close()
	dec := json.NewDecoder(fh)
	err = dec.Decode(&ci)
	if err != nil {
		return err
	}

	sections, ok := ci.(map[string]interface{})
	if !ok {
		return fmt.Errorf("configuration must be a JSON object")
	}

	for key, value := range sections {
		var err error

		switch key {
		case "listen":
			err = readListenSection(&Config.Listen, value, "listen")
		case "connect":
			err = readConnectSection(&Config, value)
		case "startup_parameters":
			err = readStartupParameterSection(&Config, value)
		case "databases":
			err = readDatabaseSection(&Config, value)
		case "prometheus":
			err = readPrometheusSection(&Config, value)
		default:
			err = fmt.Errorf("unrecognized configuration section %q", key)
		}
		if err != nil {
			return err
		}
	}

	return nil
}
