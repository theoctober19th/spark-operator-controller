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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func newSpark8tTestApp(serviceAccount *string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "spark-pi", Namespace: "analytics"},
		Spec: v1beta2.SparkApplicationSpec{
			Mode:                v1beta2.DeployModeCluster,
			MainApplicationFile: ptr.To("local:///opt/spark/examples/jars/spark-examples.jar"),
			Driver:              v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{ServiceAccount: serviceAccount}},
		},
	}
}

func TestDriverServiceAccount(t *testing.T) {
	templateWith := func(name string) *corev1.PodTemplateSpec {
		return &corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: name}}
	}

	t.Run("field takes precedence over the pod template", func(t *testing.T) {
		app := newSpark8tTestApp(ptr.To("etl"))
		app.Spec.Driver.Template = templateWith("other")
		assert.Equal(t, "etl", driverServiceAccount(app))
	})

	t.Run("falls back to the pod template", func(t *testing.T) {
		app := newSpark8tTestApp(ptr.To(""))
		app.Spec.Driver.Template = templateWith("from-template")
		assert.Equal(t, "from-template", driverServiceAccount(app))
	})

	t.Run("none", func(t *testing.T) {
		assert.Empty(t, driverServiceAccount(newSpark8tTestApp(nil)))
	})
}

func TestParseProperties(t *testing.T) {
	t.Run("parses properties, keeping '=' in values", func(t *testing.T) {
		properties, err := parseProperties("a.b=1\n\n  c.d=x=y \nspark.metrics.conf.*.sink.k=v\n")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"a.b": "1", "c.d": "x=y", "spark.metrics.conf.*.sink.k": "v"}, properties)
	})

	t.Run("rejects a line that is not a property without echoing it", func(t *testing.T) {
		_, err := parseProperties("a=1\nsupersecret\n")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "line 2")
		assert.NotContains(t, err.Error(), "supersecret")
	})
}

func TestSpark8tSubmitter_Submit(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "6443")

	hubProperties := map[string]string{
		"spark.eventLog.dir":             "s3a://bucket/events",
		"spark.hadoop.fs.s3a.secret.key": "the-secret",
		"spark.kubernetes.namespace":     "analytics",
	}

	newSubmitter := func(getConfig func(context.Context, string, string) (map[string]string, error), submit func([]string) error) *Spark8tSubmitter {
		s := NewSpark8tSubmitter(Spark8tSubmitterConfig{})
		s.getConfig = getConfig
		s.submit = submit
		return s
	}

	t.Run("passes the properties of the service account to spark-submit, then removes them", func(t *testing.T) {
		var gotUsername, gotNamespace, propertiesFile, content string
		var mode os.FileMode
		var gotArgs []string
		s := newSubmitter(
			func(_ context.Context, username, namespace string) (map[string]string, error) {
				gotUsername, gotNamespace = username, namespace
				return hubProperties, nil
			},
			func(args []string) error {
				gotArgs = args
				propertiesFile = args[1]
				data, err := os.ReadFile(propertiesFile)
				require.NoError(t, err)
				content = string(data)
				info, err := os.Stat(propertiesFile)
				require.NoError(t, err)
				mode = info.Mode().Perm()
				return nil
			},
		)

		require.NoError(t, s.Submit(context.Background(), newSpark8tTestApp(ptr.To("etl"))))

		assert.Equal(t, "etl", gotUsername)
		assert.Equal(t, "analytics", gotNamespace)
		require.GreaterOrEqual(t, len(gotArgs), 4)
		assert.Equal(t, "--properties-file", gotArgs[0])
		// The arguments generated from the SparkApplication follow the properties file, hence they win.
		assert.Equal(t, "--master", gotArgs[2])
		assert.Equal(t, "local:///opt/spark/examples/jars/spark-examples.jar", gotArgs[len(gotArgs)-1])
		assert.Equal(t,
			"spark.eventLog.dir=s3a://bucket/events\nspark.hadoop.fs.s3a.secret.key=the-secret\nspark.kubernetes.namespace=analytics\n",
			content)
		assert.Equal(t, os.FileMode(0o600), mode)
		assert.NoFileExists(t, propertiesFile)
		assert.NotContains(t, fmt.Sprint(gotArgs), "the-secret", "credentials must not be passed on the command line")
	})

	t.Run("removes the properties file when spark-submit fails and reports the error", func(t *testing.T) {
		var propertiesFile string
		s := newSubmitter(
			func(context.Context, string, string) (map[string]string, error) { return hubProperties, nil },
			func(args []string) error {
				propertiesFile = args[1]
				return fmt.Errorf("driver pod already exist")
			},
		)

		err := s.Submit(context.Background(), newSpark8tTestApp(ptr.To("etl")))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "driver pod already exist")
		assert.NoFileExists(t, propertiesFile)
	})

	t.Run("does not submit when the service account cannot be resolved", func(t *testing.T) {
		submitted := false
		s := newSubmitter(
			func(context.Context, string, string) (map[string]string, error) {
				return nil, fmt.Errorf("Account analytics:etl could not be found.")
			},
			func([]string) error { submitted = true; return nil },
		)

		err := s.Submit(context.Background(), newSpark8tTestApp(ptr.To("etl")))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not be found")
		assert.False(t, submitted)
	})

	t.Run("requires a driver service account", func(t *testing.T) {
		called := false
		s := newSubmitter(
			func(context.Context, string, string) (map[string]string, error) { called = true; return nil, nil },
			func([]string) error { called = true; return nil },
		)

		err := s.Submit(context.Background(), newSpark8tTestApp(nil))

		require.Error(t, err)
		assert.False(t, called)
	})

	t.Run("passes application arguments verbatim as single arguments, without a shell", func(t *testing.T) {
		marker := filepath.Join(t.TempDir(), "injected")
		app := newSpark8tTestApp(ptr.To("etl"))
		app.Spec.Arguments = []string{"x; touch " + marker, "--name", "two words"}
		var gotArgs []string
		s := newSubmitter(
			func(context.Context, string, string) (map[string]string, error) { return hubProperties, nil },
			func(args []string) error { gotArgs = args; return nil },
		)

		require.NoError(t, s.Submit(context.Background(), app))

		tail := gotArgs[len(gotArgs)-3:]
		assert.Equal(t, []string{"x; touch " + marker, "--name", "two words"}, tail)
		assert.NoFileExists(t, marker)
	})
}

// writeFakePython writes an executable that stands for the Python interpreter.
func writeFakePython(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-python")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o700))
	return path
}

func TestSpark8tSubmitter_getServiceAccountConfig(t *testing.T) {
	t.Run("runs spark8t without a shell and parses its output", func(t *testing.T) {
		s := NewSpark8tSubmitter(Spark8tSubmitterConfig{
			PythonBinary: writeFakePython(t, `echo "args=$*"; echo "warning" >&2`),
		})

		properties, err := s.getServiceAccountConfig(context.Background(), "etl; rm -rf /", "analytics")

		require.NoError(t, err)
		assert.Equal(t,
			"-m spark8t.cli.service_account_registry get-config --username=etl; rm -rf / --namespace=analytics",
			properties["args"])
	})

	t.Run("reports the message of spark8t when it fails", func(t *testing.T) {
		s := NewSpark8tSubmitter(Spark8tSubmitterConfig{
			PythonBinary: writeFakePython(t, `echo "Account analytics:nosuch could not be found."; exit 1`),
		})

		_, err := s.getServiceAccountConfig(context.Background(), "nosuch", "analytics")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "Account analytics:nosuch could not be found.")
	})

	t.Run("gives up after the timeout", func(t *testing.T) {
		s := NewSpark8tSubmitter(Spark8tSubmitterConfig{
			PythonBinary:     writeFakePython(t, `exec sleep 30`),
			GetConfigTimeout: 200 * time.Millisecond,
		})

		start := time.Now()
		_, err := s.getServiceAccountConfig(context.Background(), "etl", "analytics")

		require.Error(t, err)
		assert.Less(t, time.Since(start), 10*time.Second)
	})
}
