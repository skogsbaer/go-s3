package main

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	// Setup logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Infof("Starting tests...")

	// Run tests
	exitCode := m.Run()

	// Cleanup
	logrus.Infof("Tests completed")

	// Exit with the appropriate code
	os.Exit(exitCode)
}
