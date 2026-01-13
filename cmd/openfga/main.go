// Package main contains the root of all commands.
package main

import (
	"os"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/cmd/validatemodels"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := run.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	// NUMARIS: This is part of the original code, built for sql migrations
	// unuseful for dynamodb connections
	// what we do instead is verify if we can get any data from the table and that's it
	// migrateCmd := migrate.NewMigrateCommand()
	// rootCmd.AddCommand(migrateCmd)

	validateModelsCmd := validatemodels.NewValidateCommand()
	rootCmd.AddCommand(validateModelsCmd)

	versionCmd := cmd.NewVersionCommand()
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
