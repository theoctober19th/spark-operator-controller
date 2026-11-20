/*
Copyright 2026 Canonical Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const (
	defaultSpark8tPythonBinary     = "python3"
	defaultSpark8tGetConfigTimeout = 30 * time.Second
)

// Spark8tSubmitterConfig is the configuration of the Spark8tSubmitter.
type Spark8tSubmitterConfig struct {
	// PythonBinary is the Python interpreter that can import spark8t. Defaults to python3.
	PythonBinary string
	// GetConfigTimeout bounds the time spent resolving the properties of a service account.
	GetConfigTimeout time.Duration
}

// Spark8tSubmitter submits a SparkApplication by calling spark-submit with the Spark properties
// that spark8t resolves for the driver service account of the application. As with the
// spark-client tools of Charmed Apache Spark, those are the properties of the service account
// layered over the ones that the Integration Hub provides for it. The arguments generated from
// the SparkApplication are passed to spark-submit as well, hence they take precedence over them.
//
// The submitter deliberately does not run `python3 -m spark8t.cli.spark_submit`:
//   - spark8t joins the spark-submit arguments into a string that it runs through a shell, so
//     application arguments, which are user-controlled, would be interpreted by that shell;
//   - spark8t always exits with status 0, whether spark-submit succeeded or not, which would hide
//     submission failures from the controller.
//
// Instead, spark8t only provides the properties, and spark-submit is run directly as with the
// default submitter, with an argument vector and without a shell.
type Spark8tSubmitter struct {
	config Spark8tSubmitterConfig

	// getConfig and submit can be replaced in tests.
	getConfig func(ctx context.Context, username, namespace string) (map[string]string, error)
	submit    func(args []string) error
}

// Spark8tSubmitter implements SparkApplicationSubmitter interface.
var _ SparkApplicationSubmitter = &Spark8tSubmitter{}

// NewSpark8tSubmitter creates a Spark8tSubmitter.
func NewSpark8tSubmitter(config Spark8tSubmitterConfig) *Spark8tSubmitter {
	if config.PythonBinary == "" {
		config.PythonBinary = defaultSpark8tPythonBinary
	}
	if config.GetConfigTimeout <= 0 {
		config.GetConfigTimeout = defaultSpark8tGetConfigTimeout
	}
	s := &Spark8tSubmitter{config: config, submit: runSparkSubmit}
	s.getConfig = s.getServiceAccountConfig
	return s
}

// Submit implements SparkApplicationSubmitter interface.
func (s *Spark8tSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	logger := log.FromContext(ctx)

	serviceAccount := driverServiceAccount(app)
	if serviceAccount == "" {
		return fmt.Errorf("the Spark8tSubmitter requires a driver service account: set .spec.driver.serviceAccount or configure a default service account")
	}

	properties, err := s.getConfig(ctx, serviceAccount, app.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get the spark8t configuration of service account %s:%s: %v", app.Namespace, serviceAccount, err)
	}

	propertiesFile, err := writePropertiesFile(properties)
	if err != nil {
		return fmt.Errorf("failed to write the spark8t configuration of service account %s:%s: %v", app.Namespace, serviceAccount, err)
	}
	defer func() {
		if err := os.Remove(propertiesFile); err != nil {
			logger.Error(err, "Failed to remove the properties file", "file", propertiesFile)
		}
	}()

	args, err := buildSparkSubmitArgs(app, false)
	if err != nil {
		return fmt.Errorf("failed to build spark-submit arguments: %v", err)
	}
	// Properties given with --conf take precedence over the ones of the properties file. The values
	// of the properties file are not logged, since they may contain credentials.
	args = append([]string{"--properties-file", propertiesFile}, args...)

	logger.Info("Running spark-submit", "arguments", args, "spark8tProperties", len(properties))
	if err := s.submit(args); err != nil {
		return fmt.Errorf("failed to run spark-submit: %v", err)
	}

	return nil
}

// getServiceAccountConfig returns the Spark properties that spark8t resolves for a service account.
func (s *Spark8tSubmitter) getServiceAccountConfig(ctx context.Context, username, namespace string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.config.GetConfigTimeout)
	defer cancel()

	// The values are passed with the --option=value form, so that they can never be parsed as options.
	cmd := exec.CommandContext(ctx, s.config.PythonBinary,
		"-m", "spark8t.cli.service_account_registry", "get-config",
		"--username="+username, "--namespace="+namespace)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if message := strings.TrimSpace(stdout.String() + " " + stderr.String()); message != "" {
			return nil, fmt.Errorf("%s", message)
		}
		return nil, err
	}

	return parseProperties(stdout.String())
}

// driverServiceAccount returns the service account of the driver, which is looked up in the same
// places as for the default service account of the operator.
func driverServiceAccount(app *v1beta2.SparkApplication) string {
	if serviceAccount := app.Spec.Driver.ServiceAccount; serviceAccount != nil && *serviceAccount != "" {
		return *serviceAccount
	}
	if template := app.Spec.Driver.Template; template != nil {
		return template.Spec.ServiceAccountName
	}
	return ""
}

// parseProperties parses `key=value` lines, as printed by spark8t.
func parseProperties(content string) (map[string]string, error) {
	properties := make(map[string]string)
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || key == "" {
			// The line itself is not printed, since it may contain a credential.
			return nil, fmt.Errorf("line %d of the spark8t configuration is not in the key=value format", i+1)
		}
		properties[key] = value
	}
	return properties, nil
}

// writePropertiesFile writes the properties to a file that only the current user can read.
func writePropertiesFile(properties map[string]string) (string, error) {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var content strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&content, "%s=%s\n", key, properties[key])
	}

	// os.CreateTemp creates the file with mode 0600.
	file, err := os.CreateTemp("", "spark8t-*.properties")
	if err != nil {
		return "", err
	}
	if _, err := file.WriteString(content.String()); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}
