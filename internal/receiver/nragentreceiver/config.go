package nragentreceiver

import (
	"errors"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Config defines configuration for Zipkin receiver.
// Config defines configuration for the NR agent receiver.
type Config struct {
	config.ReceiverSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
	// Configures the receiver server protocol.
	confighttp.HTTPServerSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
}

var _ config.Receiver = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	// Example validation: Ensure Endpoint is not empty
	if cfg.Endpoint == "" {
		return errors.New("endpoint cannot be empty")
	}
	return nil
}
